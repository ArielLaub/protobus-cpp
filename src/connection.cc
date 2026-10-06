#include "protobus/connection.h"

#include <chrono>
#include <cmath>
#include <deque>
#include <future>
#include <random>

#include "executor.h"
#include "protobus/config.h"
#include "protobus/errors.h"
#include "protobus/logger.h"
#include "scheduler.h"
#include "uuid.h"

namespace protobus {

using amqp::FieldTable;
using amqp::FieldValue;

namespace {

int64_t nowMs() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// AMQP properties a republish copies from the original delivery.
//
// The retry and DLQ hops RE-PUBLISH a failed message, building its properties
// by hand, so anything not named here is dropped. Three are deliberately
// left out: deliveryMode is re-expressed as persistent at every site;
// expiration would race the retry queue's own TTL, or quietly delete the
// evidence on the DLQ; and userId is validated by RabbitMQ against the
// publishing connection's user, which for a republish is the consumer's.
// correlationId, messageId, replyTo and headers are set at each site, which
// disagree about them.
void carryProperties(const amqp::Properties& from, amqp::Properties& to) {
  if (from.contentType) to.contentType = from.contentType;
  if (from.contentEncoding) to.contentEncoding = from.contentEncoding;
  if (from.priority) to.priority = from.priority;
  if (from.timestamp) to.timestamp = from.timestamp;
  if (from.type) to.type = from.type;
  if (from.appId) to.appId = from.appId;
}

// Runs `fn`, or queues it behind the call already running on this thread.
// Releasing a confirm slot starts the next parked publish, whose failure
// releases its slot and starts the next: run directly, a channel closing on
// thousands of parked publishes would recurse once per publish.
thread_local std::deque<std::function<void()>>* trampoline = nullptr;

void runTrampolined(std::function<void()> fn) {
  if (trampoline != nullptr) {
    trampoline->push_back(std::move(fn));
    return;
  }
  std::deque<std::function<void()>> queue;
  queue.push_back(std::move(fn));
  trampoline = &queue;
  while (!queue.empty()) {
    auto next = std::move(queue.front());
    queue.pop_front();
    try {
      next();
    } catch (...) {
      trampoline = nullptr;
      throw;
    }
  }
  trampoline = nullptr;
}

double jitterFraction() {
  thread_local std::mt19937 gen{std::random_device{}()};
  return std::uniform_real_distribution<double>(0.0, 0.3)(gen);
}

}  // namespace

// ---- internal state ------------------------------------------------------------

// One confirmed publish, from the request to the transport's answer. The two
// ends are tracked apart: the caller is answered once (`settled`), at the
// confirm or at its deadline, while the slot under the outstanding-confirm
// bound is held until the transport resolves the publish (`resolved`): an
// ambiguous timeout says nothing about whether the broker still holds it.
struct PublishOp {
  std::atomic<bool> settled{false};
  std::atomic<bool> resolved{false};
  std::atomic<uint64_t> timer{0};
  // Guarded by the PublishState's mutex.
  bool parked = false;
  bool timedOut = false;
  uint64_t waiterId = 0;
};

struct Connection::PublishState {
  std::mutex mutex;
  std::weak_ptr<amqp::Channel> owner;
  // Publishes started on the channel that the transport has not resolved
  // yet, whether or not their callers are still waiting: what
  // MAX_OUTSTANDING_CONFIRMS bounds.
  int64_t inFlight = 0;
  // Of those, the ones whose callers already timed out.
  int64_t timedOut = 0;
  bool closed = false;
  bool retired = false;
  uint64_t nextWaiterId = 0;
  // Publishes parked on the bound, in arrival order, each started once a slot
  // frees. Bounded by MAX_PARKED_PUBLISHES.
  struct Waiter {
    std::shared_ptr<PublishOp> op;
    std::function<void()> start;
  };
  std::map<uint64_t, Waiter> waiters;
};

struct Connection::DeliveryEntry {
  AbortController controller;
  std::atomic<bool> cancelled{false};
  // A processing timeout aborts on a worker, never on the timer thread, and
  // only while the handler is still running: none of the listeners it
  // registered may START once the connection has seen it return, and that
  // return waits for one already running. What they may use stays alive
  // until then through MessageHandlerContext::keepAlive.
  std::mutex timeoutAbortMutex;
  bool handlerReturned = false;
};

struct ConsumerSpec {
  // Serialises an ordered consumer's deliveries.
  struct Strand {
    std::mutex mutex;
    std::deque<std::function<void()>> queue;
    bool running = false;
  };
  std::shared_ptr<Strand> strand;
  std::string queueName;
  MessageHandler handler;
  ConsumeOptions options;
  bool lateAck = false;
  std::optional<ConsumeRetryOptions> retry;
  std::optional<int64_t> processingTimeoutMs;
};

struct Connection::Delivery {
  std::shared_ptr<amqp::Channel> channel;
  std::shared_ptr<const ConsumerSpec> spec;
  amqp::Delivery msg;
  std::string correlationId;
  std::string replyTo;
  FieldTable headers;
  int64_t retryCount = 0;
  std::string originalRoutingKey;
  std::shared_ptr<DeliveryEntry> entry;
  std::atomic<bool> settled{false};
  std::atomic<uint64_t> timer{0};
};

// ---- construction --------------------------------------------------------------

Connection::Connection(std::shared_ptr<amqp::Transport> transport)
    : transport_(transport ? std::move(transport) : amqp::rabbitmqTransport()),
      executor_(std::make_shared<detail::Executor>()),
      scheduler_(std::make_shared<detail::Scheduler>()) {}

Connection::~Connection() {
  try {
    std::shared_ptr<amqp::Connection> h;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      manualDisconnect_ = true;
      ++generation_;
      h = std::move(handle_);
      connected_ = false;
    }
    if (h) h->close();
  } catch (...) {
  }
  executor_->shutdown(std::chrono::seconds(5));
}

detail::Executor& Connection::executor() { return *executor_; }
detail::Scheduler& Connection::scheduler() { return *scheduler_; }

// ---- state ---------------------------------------------------------------------

bool Connection::isConnected() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return connected_;
}

bool Connection::isReconnecting() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return reconnecting_;
}

bool Connection::isReady() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return ready_;
}

// ---- readiness -----------------------------------------------------------------

std::function<void()> Connection::registerRestorer(Restorer restore) {
  std::lock_guard<std::mutex> lock(restorerMutex_);
  const uint64_t id = nextRestorerId_++;
  restorers_.emplace_back(id, std::move(restore));
  std::weak_ptr<Connection> weak = weak_from_this();
  return [weak, id] {
    auto self = weak.lock();
    if (!self) return;
    std::lock_guard<std::mutex> lock(self->restorerMutex_);
    auto& list = self->restorers_;
    for (auto it = list.begin(); it != list.end(); ++it) {
      if (it->first == id) {
        list.erase(it);
        return;
      }
    }
  };
}

void Connection::whenReady(std::optional<int64_t> timeoutMs) {
  const int64_t limit = timeoutMs.value_or(Config::connectionReadyTimeoutMs());
  std::unique_lock<std::mutex> lock(mutex_);
  if (ready_) return;
  if (manualDisconnect_) throw NotReadyError("the connection has been closed");
  const uint64_t abandoned = abandonCount_;
  readyCv_.wait_for(lock, std::chrono::milliseconds(limit), [&] { return ready_ || abandonCount_ != abandoned; });
  if (ready_) return;
  if (abandonCount_ != abandoned) throw NotReadyError(abandonMessage_);
  throw NotReadyError("the connection did not become ready within " + std::to_string(limit) + "ms");
}

void Connection::markReady() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ready_ = true;
  }
  readyCv_.notify_all();
}

void Connection::markNotReady() {
  std::lock_guard<std::mutex> lock(mutex_);
  ready_ = false;
}

void Connection::abandonReady(const std::string& message) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ready_ = false;
    ++abandonCount_;
    abandonMessage_ = message;
  }
  readyCv_.notify_all();
}

// Sequential rather than concurrent: one component's restore declares the
// exchange another one binds to.
void Connection::runRestorers(uint64_t generation) {
  std::vector<Restorer> list;
  {
    std::lock_guard<std::mutex> lock(restorerMutex_);
    for (const auto& [_, r] : restorers_) list.push_back(r);
  }
  auto current = [&] {
    std::lock_guard<std::mutex> lock(mutex_);
    return generation_;
  };
  for (auto& restore : list) {
    if (generation != current()) throw ReconnectionError("connection was torn down while restoring");
    restore(generation);
  }
  if (generation != current()) throw ReconnectionError("connection was torn down while restoring");
}

// ---- connect and reconnect -----------------------------------------------------

void Connection::connect(const std::string& url, ReconnectionOptions options) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (connected_) throw AlreadyConnectedError();
    url_ = url;
    reconnection_ = options;
    manualDisconnect_ = false;
  }
  doConnect();
  // Nothing to restore on a first connect: components initialise themselves
  // against it, so the socket coming up is readiness.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    reconnecting_ = false;
  }
  markReady();
}

// Single-flight: a manual connect() racing the reconnect timer cannot open
// two sockets.
std::shared_ptr<amqp::Connection> Connection::doConnect() {
  std::lock_guard<std::mutex> flight(connectMutex_);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (connected_ && handle_) return handle_;
  }
  return connectOnce();
}

std::shared_ptr<amqp::Connection> Connection::connectOnce() {
  std::string url;
  uint64_t generation;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    url = url_;
    generation = generation_;
  }
  Logger::info("connecting to bus - " + redactUrl(url));

  std::shared_ptr<amqp::Connection> handle;
  try {
    handle = transport_->connect(url, static_cast<int>(Config::heartbeatSeconds()));
  } catch (const std::exception& e) {
    Logger::error(std::string("failed to connect: ") + e.what());
    std::lock_guard<std::mutex> lock(mutex_);
    connected_ = false;
    throw;
  }

  {
    std::unique_lock<std::mutex> lock(mutex_);
    // Anything that tore the connection down bumped the generation; a
    // connect completing afterwards belongs to a previous era.
    if (generation != generation_ || manualDisconnect_) {
      lock.unlock();
      Logger::info("discarding a connection that completed after disconnect");
      try {
        handle->close();
      } catch (const std::exception& e) {
        Logger::debug(std::string("failed closing a superseded connection: ") + e.what());
      }
      throw ReconnectionError("connection was torn down while connecting");
    }
    handle_ = handle;
    connected_ = true;
    reconnectAttempts_ = 0;
    // reconnecting_ is deliberately NOT cleared here: on the reconnect path a
    // socket is only half the job, and restoration still has to run.
  }

  std::weak_ptr<Connection> weak = weak_from_this();
  const amqp::Connection* raw = handle.get();
  handle->onClose([weak, raw](std::optional<std::string> reason) {
    if (auto self = weak.lock()) self->onHandleClosed(raw, std::move(reason));
  });
  Logger::info("connected to message bus");
  return handle;
}

void Connection::onHandleClosed(const amqp::Connection* handle, std::optional<std::string> reason) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // A discarded or replaced handle closing is not news.
    if (handle_.get() != handle) return;
    if (manualDisconnect_) {
      Logger::info("connection closed (manual disconnect)");
      return;
    }
    connected_ = false;
    ready_ = false;
    // Retire this generation: a restoration in flight against it is working
    // on a dead socket, and its next generation check aborts it.
    ++generation_;
  }
  Logger::warn("connection closed unexpectedly" + (reason ? ": " + *reason : std::string()));
  if (reason) emitError(ConnectionError(*reason));
  emitDisconnected();
  scheduleReconnect();
}

void Connection::scheduleReconnect() {
  int attempt;
  int64_t delay;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (manualDisconnect_ || reconnecting_) return;
    const auto& o = reconnection_;
    if (o.maxRetries > 0 && reconnectAttempts_ >= o.maxRetries) {
      lock.unlock();
      const std::string message =
          "max reconnection attempts (" + std::to_string(o.maxRetries) + ") exceeded";
      Logger::error(message);
      // Listeners first: a waiter released by abandonReady() may act on the
      // failure (or tear down what a listener uses) at once.
      emitError(ReconnectionError(message));
      abandonReady(message);
      return;
    }
    reconnecting_ = true;
    attempt = ++reconnectAttempts_;
    const double base = std::min(static_cast<double>(o.initialDelayMs) * std::pow(o.backoffMultiplier, attempt - 1),
                                 static_cast<double>(o.maxDelayMs));
    delay = static_cast<int64_t>(std::floor(base + jitterFraction() * base));
  }
  Logger::info("scheduling reconnection attempt " + std::to_string(attempt) + " in " + std::to_string(delay) + "ms");
  emitReconnecting(attempt, delay);
  std::weak_ptr<Connection> weak = weak_from_this();
  const uint64_t timer = scheduler_->schedule(std::chrono::milliseconds(delay), [weak] {
    if (auto self = weak.lock()) {
      self->executor_->post([weak] {
        if (auto s = weak.lock()) s->reconnectAttempt();
      });
    }
  });
  // A disconnect() between scheduling and here found no timer to cancel:
  // cancel it now rather than leave it armed.
  std::unique_lock<std::mutex> lock(mutex_);
  if (manualDisconnect_) {
    lock.unlock();
    scheduler_->cancel(timer);
    return;
  }
  reconnectTimer_ = timer;
}

void Connection::reconnectAttempt() {
  int attempt;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (manualDisconnect_) {
      reconnecting_ = false;
      return;
    }
    // Read before connecting: a successful connect resets it to zero.
    attempt = reconnectAttempts_;
  }
  try {
    doConnect();
    uint64_t generation;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      generation = generation_;
    }
    // Restoration is part of reconnecting: until every component has its
    // channel, queues and consumers back the application cannot use the
    // socket.
    runRestorers(generation);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      reconnecting_ = false;
    }
    markReady();
    Logger::info("reconnection successful after " + std::to_string(attempt) + " attempts");
    emitReconnected();
  } catch (const std::exception& e) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (manualDisconnect_) {
        reconnecting_ = false;
        return;
      }
    }
    Logger::error("reconnection attempt " + std::to_string(attempt) + " failed: " + e.what());
    // A generation that connected but could not be restored looks healthy
    // and serves nothing. Drop it and let the backoff try again.
    discardGeneration();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      reconnectAttempts_ = attempt;
      reconnecting_ = false;
    }
    scheduleReconnect();
  }
}

void Connection::discardGeneration() {
  std::shared_ptr<amqp::Connection> h;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ++generation_;
    ready_ = false;
    connected_ = false;
    h = std::move(handle_);
  }
  if (!h) return;
  try {
    h->close();
  } catch (const std::exception& e) {
    Logger::debug(std::string("failed closing an unrestorable connection: ") + e.what());
  }
}

void Connection::disconnect() {
  std::shared_ptr<amqp::Connection> h;
  uint64_t timer;
  bool wasUp;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    wasUp = !manualDisconnect_ && (connected_ || reconnecting_);
    manualDisconnect_ = true;
    // Invalidate any connect already past its timer.
    ++generation_;
    timer = reconnectTimer_;
    reconnectTimer_ = 0;
    reconnecting_ = false;
    h = handle_;
  }
  if (timer != 0) scheduler_->cancel(timer);
  abandonReady("the connection has been closed");
  if (h) h->close();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    connected_ = false;
    handle_.reset();
  }
  // Components waiting on the connection (pending calls, streams) hear of a
  // deliberate close as they would of a lost one; unlike a loss, nothing
  // reconnects afterwards.
  if (wasUp) emitDisconnected();
}

// ---- events --------------------------------------------------------------------

Connection::ListenerId Connection::onReconnecting(std::function<void(int, int64_t)> fn) {
  std::lock_guard<std::mutex> lock(listenerMutex_);
  const ListenerId id = nextListenerId_++;
  reconnectingListeners_.emplace(id, std::move(fn));
  return id;
}

Connection::ListenerId Connection::onReconnected(std::function<void()> fn) {
  std::lock_guard<std::mutex> lock(listenerMutex_);
  const ListenerId id = nextListenerId_++;
  reconnectedListeners_.emplace(id, std::move(fn));
  return id;
}

Connection::ListenerId Connection::onDisconnected(std::function<void()> fn) {
  std::lock_guard<std::mutex> lock(listenerMutex_);
  const ListenerId id = nextListenerId_++;
  disconnectedListeners_.emplace(id, std::move(fn));
  return id;
}

Connection::ListenerId Connection::onError(std::function<void(const std::exception&)> fn) {
  std::lock_guard<std::mutex> lock(listenerMutex_);
  const ListenerId id = nextListenerId_++;
  errorListeners_.emplace(id, std::move(fn));
  return id;
}

void Connection::removeListener(ListenerId id) {
  std::lock_guard<std::mutex> lock(listenerMutex_);
  reconnectingListeners_.erase(id);
  reconnectedListeners_.erase(id);
  disconnectedListeners_.erase(id);
  errorListeners_.erase(id);
}

namespace {
template <typename Map, typename... Args>
void callAll(std::mutex& m, const Map& listeners, Args&&... args) {
  std::vector<typename Map::mapped_type> copy;
  {
    std::lock_guard<std::mutex> lock(m);
    for (const auto& [_, fn] : listeners) copy.push_back(fn);
  }
  for (auto& fn : copy) {
    try {
      fn(args...);
    } catch (const std::exception& e) {
      Logger::error(std::string("protobus: a connection listener threw: ") + e.what());
    } catch (...) {
    }
  }
}
}  // namespace

void Connection::emitReconnecting(int attempt, int64_t delayMs) {
  callAll(listenerMutex_, reconnectingListeners_, attempt, delayMs);
}
void Connection::emitReconnected() { callAll(listenerMutex_, reconnectedListeners_); }
void Connection::emitDisconnected() { callAll(listenerMutex_, disconnectedListeners_); }
void Connection::emitError(const std::exception& error) { callAll(listenerMutex_, errorListeners_, error); }

// ---- channel operations ----------------------------------------------------------

std::shared_ptr<amqp::Channel> Connection::openChannel() {
  std::shared_ptr<amqp::Connection> h;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    h = handle_;
  }
  if (!h) throw NotConnectedError("cannot open a channel: not connected");
  return h->openChannel();
}

void Connection::closeChannel(const std::shared_ptr<amqp::Channel>& channel) { channel->close(); }

void Connection::declareExchange(const std::shared_ptr<amqp::Channel>& channel, const std::string& exchange,
                                 const std::string& type, const ExchangeOptions& o) {
  channel->declareExchange(exchange, type, o.durable, o.autoDelete, o.internal, o.arguments);
}

std::string Connection::declareQueue(const std::shared_ptr<amqp::Channel>& channel, const std::string& queueName,
                                     const QueueOptions& o) {
  return channel->declareQueue(queueName, o.durable, o.exclusive, o.autoDelete, o.arguments);
}

void Connection::bindQueue(const std::shared_ptr<amqp::Channel>& channel, const std::string& queue,
                           const std::string& exchange, const std::string& routingKey, const FieldTable& args) {
  channel->bindQueue(queue, exchange, routingKey, args);
}

void Connection::unbindQueue(const std::shared_ptr<amqp::Channel>& channel, const std::string& queue,
                             const std::string& exchange, const std::string& routingKey, const FieldTable& args) {
  channel->unbindQueue(queue, exchange, routingKey, args);
}

void Connection::deleteQueue(const std::shared_ptr<amqp::Channel>& channel, const std::string& queueName) {
  channel->deleteQueue(queueName);
}

void Connection::purgeQueue(const std::shared_ptr<amqp::Channel>& channel, const std::string& queueName) {
  channel->purgeQueue(queueName);
}

void Connection::ack(const std::shared_ptr<amqp::Channel>& channel, const amqp::Delivery& message) {
  channel->ack(message.deliveryTag);
}

void Connection::reject(const std::shared_ptr<amqp::Channel>& channel, const amqp::Delivery& message, bool requeue) {
  channel->reject(message.deliveryTag, requeue);
}

void Connection::cancel(const std::shared_ptr<amqp::Channel>& channel, const std::string& consumerTag) {
  channel->cancel(consumerTag);
}

// ---- publishing ------------------------------------------------------------------

std::shared_ptr<Connection::PublishState> Connection::publishStateFor(const std::shared_ptr<amqp::Channel>& channel) {
  std::shared_ptr<PublishState> state;
  {
    std::lock_guard<std::mutex> lock(publishMutex_);
    auto it = publishStates_.find(channel.get());
    if (it != publishStates_.end() && it->second->owner.lock() == channel) return it->second;
    state = std::make_shared<PublishState>();
    state->owner = channel;
    publishStates_[channel.get()] = state;
  }

  // A channel closing with publishes parked on the bound must release them,
  // or they wait forever for slots that will never free. Each is started and
  // fails against the closed channel, unsent. Registered outside the lock: on
  // a channel that has already closed, the callback runs at once, on this
  // thread.
  std::weak_ptr<Connection> weak = weak_from_this();
  std::weak_ptr<PublishState> weakState = state;
  const amqp::Channel* key = channel.get();
  channel->onClose([weak, weakState, key](const std::string&) {
    auto st = weakState.lock();
    if (!st) return;
    std::map<uint64_t, PublishState::Waiter> parked;
    {
      std::lock_guard<std::mutex> lock(st->mutex);
      st->closed = true;
      parked.swap(st->waiters);
      for (auto& [_, w] : parked) w.op->parked = false;
      st->inFlight += static_cast<int64_t>(parked.size());
    }
    for (auto& [_, w] : parked) runTrampolined(std::move(w.start));
    if (auto self = weak.lock()) {
      std::lock_guard<std::mutex> lock(self->publishMutex_);
      auto found = self->publishStates_.find(key);
      if (found != self->publishStates_.end() && found->second == st) self->publishStates_.erase(found);
    }
  });
  return state;
}

void Connection::confirmedPublish(const std::shared_ptr<amqp::Channel>& channel, const std::string& exchange,
                                  const std::string& routingKey, const std::string& content, PublishOptions options,
                                  std::function<void(std::exception_ptr)> done) {
  if (!channel) {
    done(std::make_exception_ptr(NotConnectedError("cannot publish: no channel")));
    return;
  }
  auto state = publishStateFor(channel);

  // A caller-supplied messageId survives retries, which is what lets a
  // consumer recognise a duplicate after an ambiguous outcome.
  std::string messageId = options.properties.messageId.value_or("");
  if (messageId.empty()) messageId = detail::randomUuid();
  options.properties.messageId = messageId;
  const std::string describe = (exchange.empty() ? std::string("(default)") : exchange) + " -> " + routingKey;
  const int64_t confirmTimeout = Config::publishConfirmTimeoutMs();

  auto op = std::make_shared<PublishOp>();
  auto scheduler = scheduler_;
  auto executor = executor_;
  const int64_t maxOutstanding = Config::maxOutstandingConfirms();

  // The caller's answer, exactly once.
  auto finish = [op, scheduler, done](std::exception_ptr err) {
    if (op->settled.exchange(true)) return;
    if (const uint64_t t = op->timer.load()) scheduler->cancel(t);
    done(err);
  };

  // The transport's answer, exactly once: frees the slot and starts the next
  // parked publish.
  auto release = [op, state, maxOutstanding] {
    if (op->resolved.exchange(true)) return;
    std::function<void()> next;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      --state->inFlight;
      if (op->timedOut) --state->timedOut;
      if (!state->waiters.empty() && state->inFlight < maxOutstanding) {
        auto first = state->waiters.begin();
        first->second.op->parked = false;
        next = std::move(first->second.start);
        state->waiters.erase(first);
        ++state->inFlight;
      }
    }
    if (next) runTrampolined(std::move(next));
  };

  auto start = [=]() {
    // Its caller gave up while it waited for a slot: nothing is sent, and the
    // answer it already has (ambiguous) remains true.
    if (op->settled.load()) {
      release();
      return;
    }
    try {
      channel->publish(exchange, routingKey, content, options.properties, options.mandatory,
                       [finish, release, describe, messageId](amqp::ConfirmOutcome outcome, const std::string& detail) {
                         release();
                         switch (outcome) {
                           case amqp::ConfirmOutcome::Ack:
                             finish(nullptr);
                             return;
                           case amqp::ConfirmOutcome::Returned:
                             finish(std::make_exception_ptr(UnroutableError(
                                 describe + " was confirmed but returned as unroutable", messageId)));
                             return;
                           case amqp::ConfirmOutcome::Nack:
                             finish(std::make_exception_ptr(
                                 PublishNackedError("broker nacked " + describe + ": " + detail, messageId)));
                             return;
                           case amqp::ConfirmOutcome::Closed:
                             finish(std::make_exception_ptr(ChannelClosedError(
                                 describe + " was unconfirmed when the channel closed" +
                                     (detail.empty() ? std::string() : " (" + detail + ")"),
                                 messageId)));
                             return;
                         }
                       });
    } catch (const std::exception& e) {
      release();
      finish(std::make_exception_ptr(
          ChannelClosedError(describe + " could not be published: " + std::string(e.what()), messageId)));
    }
  };

  // One deadline from the request, parked or not. Parked, the publish was
  // never sent, which is definite; sent, it is ambiguous, and it keeps its
  // slot until the transport resolves it. A channel whose every slot is held
  // that way can make no progress, so it is retired: closing it resolves what
  // it holds (as ChannelClosedError, to callers long since answered) and its
  // owner opens another, as for any channel lost on a live connection.
  std::weak_ptr<PublishState> weakState = state;
  auto onTimeout = [op, weakState, finish, describe, confirmTimeout, messageId, maxOutstanding] {
    auto st = weakState.lock();
    if (!st) return;
    bool wasParked = false;
    std::shared_ptr<amqp::Channel> retire;
    {
      std::lock_guard<std::mutex> lock(st->mutex);
      if (op->parked) {
        st->waiters.erase(op->waiterId);
        op->parked = false;
        wasParked = true;
      } else if (!op->resolved.load() && !op->timedOut) {
        op->timedOut = true;
        ++st->timedOut;
        if (st->timedOut >= maxOutstanding && !st->retired && !st->closed) {
          st->retired = true;
          retire = st->owner.lock();
        }
      }
    }
    if (wasParked) {
      finish(std::make_exception_ptr(PublishBacklogError(
          describe + " was not published: no confirm slot freed within " + std::to_string(confirmTimeout) + "ms",
          messageId)));
      return;
    }
    finish(std::make_exception_ptr(PublishConfirmTimeoutError(
        "no broker confirm for " + describe + " within " + std::to_string(confirmTimeout) + "ms", messageId)));
    if (retire) {
      Logger::warn("retiring a channel: " + std::to_string(maxOutstanding) +
                   " publish(es) on it went unconfirmed past PUBLISH_CONFIRM_TIMEOUT_MS");
      try {
        retire->close();
      } catch (const std::exception& e) {
        Logger::debug(std::string("failed closing a retired channel: ") + e.what());
      }
    }
  };
  // The timer thread must not block: answering a caller can start a parked
  // publish, which waits on the transport, and retiring closes a channel.
  op->timer = scheduler->schedule(std::chrono::milliseconds(confirmTimeout), [executor, onTimeout] {
    executor->post(onTimeout);
  });

  // Bound unconfirmed work before touching the channel at all, and bound
  // what waits behind it.
  bool now = false;
  bool refused = false;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->closed || state->inFlight < maxOutstanding) {
      ++state->inFlight;
      now = true;
    } else if (static_cast<int64_t>(state->waiters.size()) >= Config::maxParkedPublishes()) {
      refused = true;
    } else {
      op->parked = true;
      op->waiterId = ++state->nextWaiterId;
      state->waiters.emplace(op->waiterId, PublishState::Waiter{op, start});
    }
  }
  if (refused) {
    finish(std::make_exception_ptr(PublishBacklogError(
        describe + " was not published: " + std::to_string(maxOutstanding) + " publish(es) await confirms and " +
            std::to_string(Config::maxParkedPublishes()) + " more are queued behind them on this channel",
        messageId)));
    return;
  }
  if (now) runTrampolined(start);
}

void Connection::publishAsync(const std::shared_ptr<amqp::Channel>& channel, const std::string& exchange,
                              const std::string& routingKey, const std::string& content, PublishOptions options,
                              std::function<void(std::exception_ptr)> done) {
  confirmedPublish(channel, exchange, routingKey, content, std::move(options), std::move(done));
}

std::string Connection::publish(const std::shared_ptr<amqp::Channel>& channel, const std::string& exchange,
                                const std::string& routingKey, const std::string& content, PublishOptions options) {
  if (!options.properties.messageId || options.properties.messageId->empty()) {
    options.properties.messageId = detail::randomUuid();
  }
  const std::string messageId = *options.properties.messageId;
  auto promise = std::make_shared<std::promise<void>>();
  auto future = promise->get_future();
  confirmedPublish(channel, exchange, routingKey, content, std::move(options), [promise](std::exception_ptr err) {
    if (err) {
      promise->set_exception(err);
    } else {
      promise->set_value();
    }
  });
  future.get();
  return messageId;
}

std::string Connection::publishToQueue(const std::shared_ptr<amqp::Channel>& channel, const std::string& queueName,
                                       const std::string& content, PublishOptions options) {
  return publish(channel, "", queueName, content, std::move(options));
}

// ---- in-flight accounting --------------------------------------------------------

size_t Connection::inFlightDeliveries() const {
  std::lock_guard<std::mutex> lock(drainMutex_);
  return std::max(inFlightDeliveries_, runningHandlers_);
}

bool Connection::drainInFlight(int64_t timeoutMs) {
  std::unique_lock<std::mutex> lock(drainMutex_);
  return drainCv_.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                           [&] { return inFlightDeliveries_ == 0 && runningHandlers_ == 0; });
}

void Connection::deliveryStarted() {
  std::lock_guard<std::mutex> lock(drainMutex_);
  ++inFlightDeliveries_;
}

void Connection::deliveryFinished() {
  {
    std::lock_guard<std::mutex> lock(drainMutex_);
    if (inFlightDeliveries_ > 0) --inFlightDeliveries_;
  }
  drainCv_.notify_all();
}

// Counted separately from deliveries because the two come apart: a processing
// timeout settles the delivery, but the handler runs on. A drain that watched
// deliveries alone would report success while user code was still
// mid-transaction.
void Connection::handlerStarted() {
  std::lock_guard<std::mutex> lock(drainMutex_);
  ++runningHandlers_;
}

void Connection::handlerFinished() {
  {
    std::lock_guard<std::mutex> lock(drainMutex_);
    if (runningHandlers_ > 0) --runningHandlers_;
  }
  drainCv_.notify_all();
}

bool Connection::cancelStream(const std::string& correlationId) {
  std::set<std::shared_ptr<DeliveryEntry>> entries;
  {
    std::lock_guard<std::mutex> lock(deliveryMutex_);
    auto it = activeDeliveries_.find(correlationId);
    if (it == activeDeliveries_.end() || it->second.empty()) return false;
    entries = it->second;
  }
  // The same message can be in flight more than once (a redelivery
  // overlapping its predecessor), and all of them are the caller's stream.
  for (const auto& entry : entries) {
    entry->cancelled = true;
    entry->controller.abort();
  }
  Logger::debug("stream " + correlationId + " cancelled by the caller");
  return true;
}

// ---- consuming -------------------------------------------------------------------

void Connection::consume(const std::shared_ptr<amqp::Channel>& channel, const std::string& queueName,
                         MessageHandler handler, const ConsumeOptions& options, bool lateAck,
                         std::optional<ConsumeRetryOptions> retry, std::optional<int64_t> processingTimeoutMs) {
  auto spec = std::make_shared<ConsumerSpec>();
  spec->queueName = queueName;
  spec->handler = std::move(handler);
  spec->options = options;
  spec->lateAck = lateAck;
  spec->retry = std::move(retry);
  spec->processingTimeoutMs = processingTimeoutMs;
  if (options.ordered) spec->strand = std::make_shared<ConsumerSpec::Strand>();

  std::weak_ptr<Connection> weak = weak_from_this();
  std::weak_ptr<amqp::Channel> weakChannel = channel;
  channel->consume(
      queueName, options.consumerTag, options.noAck, options.exclusive,
      [weak, weakChannel, spec](amqp::Delivery msg) {
        auto self = weak.lock();
        auto ch = weakChannel.lock();
        if (!self || !ch) return;
        // Counted from arrival, for the whole settle, so a graceful shutdown
        // waits for the reply, retry or DLQ publish and not just the handler.
        self->deliveryStarted();
        auto d = std::make_shared<Delivery>();
        d->channel = std::move(ch);
        d->spec = spec;
        d->msg = std::move(msg);
        if (!spec->strand) {
          self->executor_->post([self, d] { self->handleDelivery(d); });
          return;
        }
        // Ordered: one drainer at a time works through the queue.
        auto strand = spec->strand;
        bool startDrainer = false;
        {
          std::lock_guard<std::mutex> lock(strand->mutex);
          strand->queue.push_back([self, d] { self->handleDelivery(d); });
          if (!strand->running) {
            strand->running = true;
            startDrainer = true;
          }
        }
        if (startDrainer) {
          self->executor_->post([strand] {
            for (;;) {
              std::function<void()> next;
              {
                std::lock_guard<std::mutex> lock(strand->mutex);
                if (strand->queue.empty()) {
                  strand->running = false;
                  return;
                }
                next = std::move(strand->queue.front());
                strand->queue.pop_front();
              }
              next();
            }
          });
        }
      },
      [queueName, onCancelled = options.onCancelled] {
        Logger::warn("consumer for " + queueName + " was cancelled by the broker");
        if (onCancelled) onCancelled();
      });
}

void Connection::handleDelivery(std::shared_ptr<Delivery> d) {
  const auto& spec = *d->spec;
  const auto& props = d->msg.properties;
  d->replyTo = props.replyTo.value_or("");
  d->correlationId = props.correlationId.value_or("");
  if (props.headers) d->headers = *props.headers;
  if (auto it = d->headers.find("x-retry-count"); it != d->headers.end()) {
    d->retryCount = it->second.asInt().value_or(0);
  }
  // The key the broker delivered on, never the x-original-routing-key header:
  // the header is whatever the publisher wrote, and routing the retry by it
  // would let a publisher send its message, under this consumer's
  // permissions, to a key it was never allowed to publish to. No legitimate
  // path needs the header: every port's retry and redelivery hops preserve
  // the routing key, so a retried message arrives under its original key.
  // The header stays informational, rewritten from the delivery on each hop.
  d->originalRoutingKey = d->msg.routingKey;

  Logger::debug("incoming message: " + d->msg.exchange + " " + d->msg.routingKey +
                (d->retryCount > 0 ? " (retry " + std::to_string(d->retryCount) + ")" : ""));

  // Early ackers acknowledge on delivery and never retry.
  if (!spec.options.noAck && !spec.lateAck) ack(d->channel, d->msg);

  const int64_t limit = spec.processingTimeoutMs.value_or(Config::messageProcessingTimeout());

  // Registered for the whole delivery so cancelStream() can reach the
  // handler's signal at any point.
  d->entry = std::make_shared<DeliveryEntry>();
  {
    std::lock_guard<std::mutex> lock(deliveryMutex_);
    activeDeliveries_[d->correlationId].insert(d->entry);
  }

  // The handler is raced against the timer rather than checked after it
  // returns, so a hung handler is actually interrupted: whichever finishes
  // first settles the delivery, and the other is discarded.
  auto self = shared_from_this();
  d->timer = scheduler_->schedule(std::chrono::milliseconds(limit), [self, d, limit] {
    // Only a timer that wins the race aborts: a handler that returned first
    // (a stream about to be published, say) must keep its signal.
    if (d->settled.exchange(true)) return;
    // This thread fires every timer in the process, so the handler's abort
    // listeners (application code) do not run on it, and the settlement does
    // not wait for them: a listener that blocks holds up neither other
    // timers nor the caller's PROCESSING_TIMEOUT answer. The signal fires
    // first, so a handler waiting on it wakes as the listeners start.
    auto entry = d->entry;
    self->executor_->post([entry] {
      std::lock_guard<std::mutex> lock(entry->timeoutAbortMutex);
      if (!entry->handlerReturned) entry->controller.abort();
    });
    self->executor_->post([self, d, limit] {
      self->settleError(d, std::make_exception_ptr(TimeoutError(
                               "message " + d->correlationId + " exceeded the " + std::to_string(limit) +
                               "ms processing timeout")));
    });
  });

  handlerStarted();
  MessageHandlerResult result;
  std::exception_ptr error;
  MessageHandlerContext context;
  context.signal = d->entry->controller.signal();
  context.routingKey = d->msg.routingKey;
  context.messageId = props.messageId.value_or("");
  context.redelivered = d->msg.redelivered;
  context.headers = &d->headers;
  try {
    result = spec.handler(d->msg.body, d->correlationId, context);
  } catch (...) {
    error = std::current_exception();
  }
  {
    // Waits out a timeout abort whose listeners are running, and stops one
    // that has not started yet. Only then is what they may use released.
    std::lock_guard<std::mutex> lock(d->entry->timeoutAbortMutex);
    d->entry->handlerReturned = true;
  }
  context.keepAlive.reset();
  handlerFinished();

  if (d->settled.exchange(true)) {
    // The processing timeout already settled this delivery; a late result
    // goes nowhere.
    return;
  }
  scheduler_->cancel(d->timer.load());
  if (error) {
    settleError(d, error);
  } else {
    settle(d, std::move(result));
  }
}

void Connection::settle(const std::shared_ptr<Delivery>& d, MessageHandlerResult result) {
  const auto& spec = *d->spec;
  try {
    // The reply is published before the request is settled, so the worst
    // case is a redelivered request (at-least-once, which the retry path
    // already assumes) rather than a settled request whose reply was never
    // sent.
    if (!d->replyTo.empty()) {
      if (auto* chunks = std::get_if<Generator<std::string>>(&result)) {
        auto entry = d->entry;
        publishStreamReply(d->channel, d->replyTo, d->correlationId, *chunks,
                           [entry] { return entry->cancelled.load(); });
      } else if (auto* reply = std::get_if<std::string>(&result)) {
        PublishOptions p;
        p.properties.contentType = "application/octet-stream";
        p.properties.correlationId = d->correlationId;
        publish(d->channel, Config::callbacksExchangeName(), d->replyTo, *reply, std::move(p));
      }
    }
    if (!spec.options.noAck && spec.lateAck) ack(d->channel, d->msg);
  } catch (...) {
    settleError(d, std::current_exception());
    return;
  }
  finishDelivery(d);
}

void Connection::finishDelivery(const std::shared_ptr<Delivery>& d) {
  if (d->entry) {
    // Only this attempt's entry: a concurrent redelivery of the same message
    // has its own and must keep it.
    std::lock_guard<std::mutex> lock(deliveryMutex_);
    auto it = activeDeliveries_.find(d->correlationId);
    if (it != activeDeliveries_.end()) {
      it->second.erase(d->entry);
      if (it->second.empty()) activeDeliveries_.erase(it);
    }
  }
  deliveryFinished();
}

void Connection::settleError(const std::shared_ptr<Delivery>& d, std::exception_ptr error) {
  const auto& spec = *d->spec;
  try {
    // A cancelled delivery is a normal outcome: the caller asked to stop.
    if (d->entry && d->entry->cancelled) {
      Logger::debug("message " + d->correlationId + " ended because its stream was cancelled");
      if (!spec.options.noAck && spec.lateAck) ack(d->channel, d->msg);
      finishDelivery(d);
      return;
    }

    // Unwrap a pre-encoded reply.
    std::exception_ptr original = error;
    std::optional<std::string> errorReply;
    try {
      std::rethrow_exception(error);
    } catch (const ErrorWithReply& w) {
      original = w.cause();
      errorReply = w.reply();
    } catch (...) {
    }

    try {
      std::rethrow_exception(original);
    } catch (const std::exception& err) {
      Logger::error("unhandled error consuming bus message - " + std::string(err.what()));
      if (!errorReply && spec.options.buildErrorReply) {
        try {
          errorReply = spec.options.buildErrorReply(d->msg.body, err);
        } catch (const std::exception& e) {
          Logger::warn(std::string("failed to encode the error reply: ") + e.what());
        }
      }

      // Best effort, deliberately: every terminal path answers the caller and
      // THEN settles the message. A reply that cannot be published must not
      // take the settlement with it, or the message stays unacknowledged and
      // can never reach the DLQ. Of the two, the reply is what may be lost:
      // the caller has a timeout, the DLQ is the only durable record.
      auto publishErrorReply = [&] {
        if (d->replyTo.empty() || !errorReply) return;
        try {
          PublishOptions p;
          p.properties.contentType = "application/octet-stream";
          p.properties.correlationId = d->correlationId;
          publish(d->channel, Config::callbacksExchangeName(), d->replyTo, *errorReply, std::move(p));
        } catch (const std::exception& replyErr) {
          Logger::error("failed to publish the error reply for " + d->correlationId + " to " + d->replyTo + ": " +
                        replyErr.what() + ". The caller will time out; settling the message anyway so it reaches "
                        "the DLQ.");
        }
      };

      if (!spec.options.noAck && spec.lateAck) {
        const auto& retry = spec.retry;
        const bool isHandled = retry && retry->isHandledError ? retry->isHandledError(err) : false;

        if (retry && !isHandled && retry->maxRetries > 0) {
          const auto firstFailure = d->headers.count("x-first-failure-time") &&
                                            d->headers.at("x-first-failure-time").asInt().value_or(0) != 0
                                        ? d->headers.at("x-first-failure-time")
                                        : FieldValue::fromInt(nowMs());
          if (d->retryCount < retry->maxRetries) {
            // Park the message on the retry queue. It is published to the
            // retry EXCHANGE under the original routing key, so the key
            // survives the TTL expiry and the dead-letter hop back to the
            // service's queue. The caller stays parked: no reply here.
            const int64_t next = d->retryCount + 1;
            Logger::warn("retrying message " + d->correlationId + " (attempt " + std::to_string(next) + "/" +
                         std::to_string(retry->maxRetries) + ")");
            FieldTable headers = d->headers;
            headers["x-retry-count"] = FieldValue::fromInt(next);
            headers["x-original-routing-key"] = FieldValue::fromString(d->originalRoutingKey);
            headers["x-first-failure-time"] = firstFailure;
            headers["x-last-error"] = FieldValue::fromString(safeErrorSummary(&err));

            PublishOptions p;
            p.properties.deliveryMode = 2;
            p.properties.correlationId = d->correlationId;
            // Carried through so the retried copy is recognisable as the same
            // logical message.
            p.properties.messageId = d->msg.properties.messageId;
            if (!d->replyTo.empty()) p.properties.replyTo = d->replyTo;
            p.properties.headers = std::move(headers);
            carryProperties(d->msg.properties, p.properties);
            p.mandatory = true;
            if (!retry->retryExchangeName.empty()) {
              publish(d->channel, retry->retryExchangeName, d->originalRoutingKey, d->msg.body, std::move(p));
            } else {
              publishToQueue(d->channel, retry->retryQueueName, d->msg.body, std::move(p));
            }
            ack(d->channel, d->msg);
          } else {
            // Retries spent: a terminal failure. Answer the caller, then keep
            // the message on the DLQ for an operator.
            Logger::error("message " + d->correlationId + " exceeded max retries (" +
                          std::to_string(retry->maxRetries) + "), sending to DLQ");
            publishErrorReply();

            FieldTable headers = d->headers;
            headers["x-retry-count"] = FieldValue::fromInt(d->retryCount);
            headers["x-original-routing-key"] = FieldValue::fromString(d->originalRoutingKey);
            headers["x-original-queue"] = FieldValue::fromString(spec.queueName);
            headers["x-first-failure-time"] = firstFailure;
            headers["x-dlq-time"] = FieldValue::fromInt(nowMs());
            headers["x-last-error"] = FieldValue::fromString(safeErrorSummary(&err));

            PublishOptions p;
            p.properties.deliveryMode = 2;
            p.properties.correlationId = d->correlationId;
            p.properties.messageId = d->msg.properties.messageId;
            p.properties.headers = std::move(headers);
            carryProperties(d->msg.properties, p.properties);
            p.mandatory = true;
            publishToQueue(d->channel, retry->dlqName, d->msg.body, std::move(p));
            ack(d->channel, d->msg);
          }
        } else {
          // No retry (disabled, or a handled error): answer, then reject
          // without requeue so the message does not loop.
          if (isHandled) {
            Logger::warn("handled error for message " + d->correlationId + ", not retrying: " + err.what());
          }
          publishErrorReply();
          Logger::warn("rejecting message " + d->correlationId);
          reject(d->channel, d->msg, false);
        }
      } else {
        // Early-ack or noAck: the message is already acknowledged, so retry
        // is impossible, but the caller must still be told.
        publishErrorReply();
      }
    } catch (...) {
      // Not a std::exception: nothing to describe, so it is dropped as the
      // handler's own failure would be.
      Logger::error("a handler threw a non-standard exception; rejecting the message");
      if (!spec.options.noAck && spec.lateAck) reject(d->channel, d->msg, false);
    }
  } catch (const std::exception& settleErr) {
    // Settlement itself failed (the retry or DLQ publish, say). The message is
    // still unacknowledged; hand it back to the broker after a pause, rather
    // than holding its prefetch slot until the channel closes.
    Logger::error("failed to settle message on " + spec.queueName + ": " + settleErr.what() +
                  ". Requeueing it in 1s.");
    if (!spec.options.noAck && spec.lateAck) {
      auto self = shared_from_this();
      auto channel = d->channel;
      auto msg = d->msg;
      scheduler_->schedule(std::chrono::milliseconds(1000), [self, channel, msg] {
        try {
          self->reject(channel, msg, true);
        } catch (...) {
        }
      });
    }
  }
  finishDelivery(d);
}

void Connection::publishStreamReply(const std::shared_ptr<amqp::Channel>& channel, const std::string& replyTo,
                                    const std::string& correlationId, Generator<std::string>& chunks,
                                    const std::function<bool()>& isCancelled) {
  auto publishOne = [&](const std::string& body, int64_t seq, bool final) {
    PublishOptions p;
    p.properties.contentType = "application/octet-stream";
    p.properties.correlationId = correlationId;
    FieldTable headers;
    headers[Config::HEADER_FINAL] = FieldValue::fromBool(final);
    headers[Config::HEADER_SEQ] = FieldValue::fromInt(seq);
    p.properties.headers = std::move(headers);
    publish(channel, Config::callbacksExchangeName(), replyTo, body, std::move(p));
  };

  // Look-ahead by one, so the chunk the producer yields last carries the
  // final flag without an extra empty terminal message.
  int64_t seq = 0;
  std::optional<std::string> buffered;
  while (auto chunk = chunks.next()) {
    // A caller that cancelled is not listening: stop sending and stop
    // pulling. The generator is destroyed by our caller, which unwinds it.
    if (isCancelled()) {
      Logger::debug("stream " + correlationId + " cancelled after " + std::to_string(seq) +
                    " chunk(s); not publishing further");
      return;
    }
    if (buffered) {
      publishOne(*buffered, seq, false);
      ++seq;
    }
    buffered = std::move(*chunk);
  }
  if (isCancelled()) return;
  if (buffered) {
    publishOne(*buffered, seq, true);
  } else {
    // Empty stream: a terminal-only marker so the caller's iterator ends.
    publishOne(std::string(), 0, true);
  }
}

}  // namespace protobus
