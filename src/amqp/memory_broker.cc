#include "protobus/testing/memory_broker.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <thread>

#include "threading.h"
#include "uuid.h"

namespace protobus::testing {

using amqp::AmqpError;
using amqp::ConfirmCallback;
using amqp::ConfirmOutcome;
using amqp::Delivery;
using amqp::DeliveryCallback;
using amqp::FieldTable;
using amqp::FieldValue;
using amqp::Properties;

namespace {

using Clock = std::chrono::steady_clock;

struct Message {
  std::string body;
  Properties properties;
  std::string exchange;
  std::string routingKey;
  bool redelivered = false;
  std::optional<Clock::time_point> expiry;
  uint8_t priority = 0;
};

// RabbitMQ topic matching: `*` is exactly one word, `#` zero or more.
bool topicMatch(const std::vector<std::string>& pattern, size_t pi, const std::vector<std::string>& words, size_t wi) {
  if (pi == pattern.size()) return wi == words.size();
  if (pattern[pi] == "#") {
    for (size_t k = wi; k <= words.size(); ++k) {
      if (topicMatch(pattern, pi + 1, words, k)) return true;
    }
    return false;
  }
  if (wi == words.size()) return false;
  if (pattern[pi] == "*" || pattern[pi] == words[wi]) return topicMatch(pattern, pi + 1, words, wi + 1);
  return false;
}

std::vector<std::string> splitWords(const std::string& s) {
  std::vector<std::string> out;
  size_t start = 0;
  for (;;) {
    const size_t dot = s.find('.', start);
    if (dot == std::string::npos) {
      out.push_back(s.substr(start));
      return out;
    }
    out.push_back(s.substr(start, dot - start));
    start = dot + 1;
  }
}

bool routes(const std::string& type, const std::string& bindingKey, const std::string& routingKey) {
  if (type == "fanout") return true;
  if (type == "direct") return bindingKey == routingKey;
  if (type == "topic") return topicMatch(splitWords(bindingKey), 0, splitWords(routingKey), 0);
  return false;
}

// Queue arguments compare as RabbitMQ compares them: integers of any width
// are one class.
bool equivalentArgs(const FieldTable& a, const FieldTable& b) {
  if (a.size() != b.size()) return false;
  for (const auto& [k, v] : a) {
    auto it = b.find(k);
    if (it == b.end()) return false;
    auto ai = v.asInt();
    auto bi = it->second.asInt();
    const bool aNum = v.kind != FieldValue::Kind::String && ai.has_value();
    const bool bNum = it->second.kind != FieldValue::Kind::String && bi.has_value();
    if (aNum || bNum) {
      if (!(aNum && bNum && *ai == *bi)) return false;
      continue;
    }
    if (!(v == it->second)) return false;
  }
  return true;
}

}  // namespace

struct MemConnection;
struct MemChannel;

struct Exchange {
  std::string type;
  bool durable = true;
  bool autoDelete = false;
};

struct Binding {
  std::string exchange;
  std::string queue;
  std::string key;
};

struct ConsumerRec {
  std::string tag;
  std::weak_ptr<MemChannel> channel;
  bool noAck = false;
  DeliveryCallback onDelivery;
  std::function<void()> onCancel;
};

struct Queue {
  std::string name;
  bool durable = false;
  bool exclusive = false;
  bool autoDelete = false;
  FieldTable arguments;
  MemConnection* owner = nullptr;  // exclusive owner
  std::vector<std::deque<Message>> levels{1};
  std::vector<ConsumerRec> consumers;
  size_t nextConsumer = 0;
  bool everConsumed = false;

  std::optional<int64_t> ttl() const {
    auto it = arguments.find("x-message-ttl");
    if (it == arguments.end()) return std::nullopt;
    return it->second.asInt();
  }
  std::optional<std::string> dlx() const {
    auto it = arguments.find("x-dead-letter-exchange");
    if (it == arguments.end()) return std::nullopt;
    return it->second.asString();
  }
  std::optional<std::string> dlrk() const {
    auto it = arguments.find("x-dead-letter-routing-key");
    if (it == arguments.end()) return std::nullopt;
    return it->second.asString();
  }
  int maxPriority() const {
    auto it = arguments.find("x-max-priority");
    if (it == arguments.end()) return 0;
    auto v = it->second.asInt();
    return v ? static_cast<int>(std::clamp<int64_t>(*v, 0, 255)) : 0;
  }
  size_t depth() const {
    size_t n = 0;
    for (const auto& l : levels) n += l.size();
    return n;
  }
};

struct Unacked {
  std::string queue;
  Message message;
  std::string consumerTag;
};

struct PendingPublish {
  ConfirmCallback callback;
};

struct MemChannel : std::enable_shared_from_this<MemChannel> {
  std::weak_ptr<MemoryBroker::Impl> broker;
  MemConnection* connection = nullptr;
  bool closed = false;
  std::string closeReason;
  uint16_t prefetch = 0;
  uint64_t nextDeliveryTag = 1;
  std::map<uint64_t, Unacked> unacked;
  std::vector<PendingPublish> pending;  // ConfirmMode::Drop
  std::vector<std::function<void(const std::string&)>> closeCallbacks;
  std::set<std::string> consumerTags;
  std::set<std::string> touchedQueues;

  size_t unackedFor(const std::string& tag) const {
    size_t n = 0;
    for (const auto& [_, u] : unacked) n += u.consumerTag == tag ? 1 : 0;
    return n;
  }
};

struct MemConnection {
  std::weak_ptr<MemoryBroker::Impl> broker;
  bool closed = false;
  std::vector<std::shared_ptr<MemChannel>> channels;
  std::vector<std::function<void(std::optional<std::string>)>> closeCallbacks;
};

struct MemoryBroker::Impl : std::enable_shared_from_this<MemoryBroker::Impl> {
  mutable std::mutex mutex;
  std::map<std::string, Exchange> exchanges;
  std::map<std::string, Queue> queues;
  std::vector<Binding> bindingList;
  std::vector<std::shared_ptr<MemConnection>> connections;
  ConfirmMode confirmMode = ConfirmMode::Ack;
  bool refuse = false;
  size_t attempts = 0;

  // Dispatcher: callbacks in order, on one thread.
  std::mutex dispatchMutex;
  std::condition_variable dispatchCv;
  std::deque<std::function<void()>> callbacks;
  size_t dispatched = 0;
  size_t enqueued = 0;
  bool stopping = false;
  std::thread dispatcher;
  std::atomic<std::thread::id> dispatcherId{};

  // TTL expiry.
  std::condition_variable expiryCv;
  std::thread expirer;

  Impl() {
    exchanges[""] = Exchange{"direct", true, false};
    exchanges["amq.direct"] = Exchange{"direct", true, false};
    exchanges["amq.topic"] = Exchange{"topic", true, false};
    exchanges["amq.fanout"] = Exchange{"fanout", true, false};
  }

  // The threads own the broker's state, so the broker may be destroyed from
  // inside one of its own callbacks.
  void start() {
    auto self = shared_from_this();
    dispatcher = std::thread([self] { self->dispatchLoop(); });
    expirer = std::thread([self] { self->expiryLoop(); });
  }

  void stop() {
    {
      std::lock_guard<std::mutex> lock(dispatchMutex);
      stopping = true;
    }
    dispatchCv.notify_all();
    {
      std::lock_guard<std::mutex> lock(mutex);
    }
    expiryCv.notify_all();
    for (auto* t : {&dispatcher, &expirer}) {
      if (!t->joinable()) continue;
      if (t->get_id() == std::this_thread::get_id()) {
        t->detach();
      } else {
        t->join();
      }
    }
  }

  void post(std::function<void()> fn) {
    {
      std::lock_guard<std::mutex> lock(dispatchMutex);
      callbacks.push_back(std::move(fn));
      ++enqueued;
    }
    dispatchCv.notify_all();
  }

  void dispatchLoop() {
    dispatcherId = std::this_thread::get_id();
    protobus::detail::markNonBlockingThread("the memory broker's callback thread, which stands in for a transport thread");
    std::unique_lock<std::mutex> lock(dispatchMutex);
    for (;;) {
      dispatchCv.wait(lock, [&] { return stopping || !callbacks.empty(); });
      if (callbacks.empty() && stopping) return;
      auto fn = std::move(callbacks.front());
      callbacks.pop_front();
      lock.unlock();
      try {
        fn();
      } catch (...) {
      }
      fn = nullptr;
      lock.lock();
      ++dispatched;
      dispatchCv.notify_all();
    }
  }

  void flush() {
    std::unique_lock<std::mutex> lock(dispatchMutex);
    const size_t target = enqueued;
    if (std::this_thread::get_id() == dispatcherId.load()) return;
    dispatchCv.wait(lock, [&] { return dispatched >= target || stopping; });
  }

  void requireNotDispatcher(const char* what) const {
    if (std::this_thread::get_id() == dispatcherId.load()) {
      throw std::logic_error(std::string("MemoryBroker: ") + what +
                             " waits for a broker reply and must not be called from a transport callback");
    }
  }

  void expiryLoop() {
    std::unique_lock<std::mutex> lock(mutex);
    for (;;) {
      {
        std::lock_guard<std::mutex> d(dispatchMutex);
        if (stopping) return;
      }
      std::optional<Clock::time_point> next;
      const auto now = Clock::now();
      for (auto& [name, q] : queues) {
        bool expiredAny = false;
        for (auto& level : q.levels) {
          for (auto it = level.begin(); it != level.end();) {
            if (it->expiry && *it->expiry <= now) {
              Message m = std::move(*it);
              it = level.erase(it);
              deadLetterLocked(q, std::move(m), "expired");
              expiredAny = true;
            } else {
              if (it->expiry && (!next || *it->expiry < *next)) next = it->expiry;
              ++it;
            }
          }
        }
        (void)expiredAny;
      }
      pumpLocked();
      if (next) {
        expiryCv.wait_until(lock, *next);
      } else {
        expiryCv.wait_for(lock, std::chrono::milliseconds(50));
      }
    }
  }

  // ---- routing -----------------------------------------------------------------

  std::vector<std::string> routeLocked(const std::string& exchange, const std::string& routingKey) {
    std::vector<std::string> out;
    if (exchange.empty()) {
      if (queues.count(routingKey)) out.push_back(routingKey);
      return out;
    }
    auto ex = exchanges.find(exchange);
    if (ex == exchanges.end()) return out;
    for (const auto& b : bindingList) {
      if (b.exchange != exchange) continue;
      if (!routes(ex->second.type, b.key, routingKey)) continue;
      if (std::find(out.begin(), out.end(), b.queue) == out.end()) out.push_back(b.queue);
    }
    return out;
  }

  void enqueueLocked(Queue& q, Message m, bool front = false) {
    const int max = q.maxPriority();
    if (static_cast<int>(q.levels.size()) != max + 1) q.levels.resize(max + 1);
    const int p = max > 0 ? std::min<int>(m.properties.priority.value_or(0), max) : 0;
    m.priority = static_cast<uint8_t>(p);
    if (!front) {
      std::optional<int64_t> ttl = q.ttl();
      if (m.properties.expiration) {
        try {
          const int64_t perMessage = std::stoll(*m.properties.expiration);
          ttl = ttl ? std::min(*ttl, perMessage) : perMessage;
        } catch (...) {
        }
      }
      m.expiry = ttl ? std::optional<Clock::time_point>(Clock::now() + std::chrono::milliseconds(*ttl))
                     : std::nullopt;
    }
    if (front) {
      q.levels[p].push_front(std::move(m));
    } else {
      q.levels[p].push_back(std::move(m));
    }
    expiryCv.notify_all();
  }

  void deadLetterLocked(Queue& q, Message m, const char* reason) {
    auto dlx = q.dlx();
    if (!dlx) return;
    const std::string key = q.dlrk().value_or(m.routingKey);
    Message copy;
    copy.body = std::move(m.body);
    copy.properties = std::move(m.properties);
    // RabbitMQ strips a per-message expiration when it dead-letters.
    copy.properties.expiration.reset();
    if (!copy.properties.headers) copy.properties.headers = FieldTable{};
    FieldTable death;
    death["queue"] = FieldValue::fromString(q.name);
    death["reason"] = FieldValue::fromString(reason);
    death["exchange"] = FieldValue::fromString(m.exchange);
    (*copy.properties.headers)["x-death"] = FieldValue::fromArray({FieldValue::fromTable(death)});
    copy.exchange = *dlx;
    copy.routingKey = key;
    for (const auto& target : routeLocked(*dlx, key)) {
      auto it = queues.find(target);
      if (it != queues.end()) enqueueLocked(it->second, copy);
    }
  }

  // ---- delivery ------------------------------------------------------------------

  void pumpLocked() {
    for (auto& [name, q] : queues) {
      bool progress = true;
      while (progress && q.depth() > 0 && !q.consumers.empty()) {
        progress = false;
        for (size_t attempt = 0; attempt < q.consumers.size() && q.depth() > 0; ++attempt) {
          const size_t idx = q.nextConsumer % q.consumers.size();
          q.nextConsumer = idx + 1;
          ConsumerRec& c = q.consumers[idx];
          auto ch = c.channel.lock();
          if (!ch || ch->closed) continue;
          if (!c.noAck && ch->prefetch > 0 && ch->unackedFor(c.tag) >= ch->prefetch) continue;
          Message m = popLocked(q);
          deliverLocked(q, c, ch, std::move(m));
          progress = true;
        }
      }
    }
  }

  Message popLocked(Queue& q) {
    for (int p = static_cast<int>(q.levels.size()) - 1; p >= 0; --p) {
      if (!q.levels[p].empty()) {
        Message m = std::move(q.levels[p].front());
        q.levels[p].pop_front();
        return m;
      }
    }
    return Message{};
  }

  void deliverLocked(Queue& q, ConsumerRec& c, const std::shared_ptr<MemChannel>& ch, Message m) {
    Delivery d;
    d.body = m.body;
    d.properties = m.properties;
    d.exchange = m.exchange;
    d.routingKey = m.routingKey;
    d.consumerTag = c.tag;
    d.redelivered = m.redelivered;
    d.deliveryTag = ch->nextDeliveryTag++;
    if (!c.noAck) ch->unacked.emplace(d.deliveryTag, Unacked{q.name, std::move(m), c.tag});
    auto cb = c.onDelivery;
    std::weak_ptr<MemChannel> weak = ch;
    post([cb, weak, d = std::move(d)]() mutable {
      auto live = weak.lock();
      if (!live) return;
      cb(std::move(d));
    });
  }

  void requeueLocked(Unacked u) {
    auto it = queues.find(u.queue);
    if (it == queues.end()) return;
    u.message.redelivered = true;
    enqueueLocked(it->second, std::move(u.message), true);
  }

  // ---- channel and connection teardown -------------------------------------------

  void closeChannelLocked(const std::shared_ptr<MemChannel>& ch, const std::string& reason) {
    if (ch->closed) return;
    ch->closed = true;
    ch->closeReason = reason;
    for (auto& [name, q] : queues) {
      q.consumers.erase(std::remove_if(q.consumers.begin(), q.consumers.end(),
                                       [&](const ConsumerRec& c) {
                                         auto live = c.channel.lock();
                                         return !live || live == ch;
                                       }),
                        q.consumers.end());
    }
    auto unacked = std::move(ch->unacked);
    ch->unacked.clear();
    for (auto& [_, u] : unacked) requeueLocked(std::move(u));
    auto pending = std::move(ch->pending);
    ch->pending.clear();
    auto callbacks = std::move(ch->closeCallbacks);
    ch->closeCallbacks.clear();
    post([pending = std::move(pending), callbacks = std::move(callbacks), reason]() mutable {
      for (auto& p : pending) p.callback(ConfirmOutcome::Closed, reason);
      for (auto& cb : callbacks) cb(reason);
    });
    autoDeleteLocked();
    pumpLocked();
  }

  void closeConnectionLocked(MemConnection* conn, std::optional<std::string> reason) {
    if (conn->closed) return;
    conn->closed = true;
    for (auto& ch : conn->channels) {
      closeChannelLocked(ch, reason.value_or("the connection was closed"));
    }
    for (auto it = queues.begin(); it != queues.end();) {
      if (it->second.exclusive && it->second.owner == conn) {
        removeQueueLocked(it->first);
        it = queues.begin();
      } else {
        ++it;
      }
    }
    auto callbacks = std::move(conn->closeCallbacks);
    conn->closeCallbacks.clear();
    post([callbacks = std::move(callbacks), reason]() mutable {
      for (auto& cb : callbacks) cb(reason);
    });
    connections.erase(std::remove_if(connections.begin(), connections.end(),
                                     [&](const std::shared_ptr<MemConnection>& c) { return c.get() == conn; }),
                      connections.end());
  }

  void removeQueueLocked(const std::string& name) {
    auto it = queues.find(name);
    if (it == queues.end()) return;
    auto consumers = std::move(it->second.consumers);
    queues.erase(it);
    bindingList.erase(std::remove_if(bindingList.begin(), bindingList.end(),
                                     [&](const Binding& b) { return b.queue == name; }),
                      bindingList.end());
    for (auto& c : consumers) {
      if (c.onCancel) {
        auto cb = c.onCancel;
        post([cb] { cb(); });
      }
    }
  }

  void autoDeleteLocked() {
    for (auto it = queues.begin(); it != queues.end();) {
      if (it->second.autoDelete && it->second.everConsumed && it->second.consumers.empty()) {
        const std::string name = it->first;
        ++it;
        removeQueueLocked(name);
      } else {
        ++it;
      }
    }
  }

  void channelError(const std::shared_ptr<MemChannel>& ch, int code, const std::string& text) {
    closeChannelLocked(ch, std::to_string(code) + " " + text);
    throw AmqpError(text, code, true);
  }
};

namespace {

using Impl = MemoryBroker::Impl;

class MemChannelHandle : public amqp::Channel {
 public:
  MemChannelHandle(std::shared_ptr<Impl> broker, std::shared_ptr<MemChannel> ch)
      : broker_(std::move(broker)), ch_(std::move(ch)) {}

  void declareExchange(const std::string& name, const std::string& type, bool durable, bool autoDelete, bool,
                       const FieldTable&) override {
    broker_->requireNotDispatcher("declareExchange");
    std::lock_guard<std::mutex> lock(broker_->mutex);
    requireOpen();
    auto it = broker_->exchanges.find(name);
    if (it != broker_->exchanges.end()) {
      if (it->second.type != type || it->second.durable != durable) {
        broker_->channelError(ch_, 406,
                              "PRECONDITION_FAILED - inequivalent arg 'type' for exchange '" + name + "'");
      }
      return;
    }
    broker_->exchanges[name] = Exchange{type, durable, autoDelete};
  }

  std::string declareQueue(const std::string& requested, bool durable, bool exclusive, bool autoDelete,
                           const FieldTable& arguments) override {
    broker_->requireNotDispatcher("declareQueue");
    std::lock_guard<std::mutex> lock(broker_->mutex);
    requireOpen();
    const std::string name = requested.empty() ? "amq.gen-" + detail::randomUuid() : requested;
    auto it = broker_->queues.find(name);
    if (it != broker_->queues.end()) {
      Queue& q = it->second;
      if (q.exclusive && q.owner != ch_->connection) {
        broker_->channelError(ch_, 405, "RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '" +
                                            name + "'");
      }
      if (q.durable != durable || !equivalentArgs(q.arguments, arguments)) {
        broker_->channelError(ch_, 406, "PRECONDITION_FAILED - inequivalent arg for queue '" + name + "'");
      }
      ch_->touchedQueues.insert(name);
      return name;
    }
    Queue q;
    q.name = name;
    q.durable = durable;
    q.exclusive = exclusive;
    q.autoDelete = autoDelete;
    q.arguments = arguments;
    q.owner = exclusive ? ch_->connection : nullptr;
    q.levels.resize(q.maxPriority() + 1);
    broker_->queues.emplace(name, std::move(q));
    ch_->touchedQueues.insert(name);
    return name;
  }

  void bindQueue(const std::string& queue, const std::string& exchange, const std::string& key,
                 const FieldTable&) override {
    broker_->requireNotDispatcher("bindQueue");
    std::lock_guard<std::mutex> lock(broker_->mutex);
    requireOpen();
    if (!broker_->queues.count(queue)) broker_->channelError(ch_, 404, "NOT_FOUND - no queue '" + queue + "'");
    if (!broker_->exchanges.count(exchange)) {
      broker_->channelError(ch_, 404, "NOT_FOUND - no exchange '" + exchange + "'");
    }
    for (const auto& b : broker_->bindingList) {
      if (b.exchange == exchange && b.queue == queue && b.key == key) return;
    }
    broker_->bindingList.push_back(Binding{exchange, queue, key});
  }

  void unbindQueue(const std::string& queue, const std::string& exchange, const std::string& key,
                   const FieldTable&) override {
    broker_->requireNotDispatcher("unbindQueue");
    std::lock_guard<std::mutex> lock(broker_->mutex);
    requireOpen();
    auto& list = broker_->bindingList;
    list.erase(std::remove_if(list.begin(), list.end(),
                              [&](const Binding& b) {
                                return b.exchange == exchange && b.queue == queue && b.key == key;
                              }),
               list.end());
  }

  void deleteQueue(const std::string& name) override {
    broker_->requireNotDispatcher("deleteQueue");
    std::lock_guard<std::mutex> lock(broker_->mutex);
    requireOpen();
    broker_->removeQueueLocked(name);
  }

  void purgeQueue(const std::string& name) override {
    broker_->requireNotDispatcher("purgeQueue");
    std::lock_guard<std::mutex> lock(broker_->mutex);
    requireOpen();
    auto it = broker_->queues.find(name);
    if (it == broker_->queues.end()) broker_->channelError(ch_, 404, "NOT_FOUND - no queue '" + name + "'");
    for (auto& l : it->second.levels) l.clear();
  }

  void prefetch(uint16_t count) override {
    broker_->requireNotDispatcher("prefetch");
    std::lock_guard<std::mutex> lock(broker_->mutex);
    requireOpen();
    ch_->prefetch = count;
    broker_->pumpLocked();
  }

  std::string consume(const std::string& queue, const std::string& tag, bool noAck, bool exclusive,
                      DeliveryCallback onDelivery, std::function<void()> onCancel) override {
    broker_->requireNotDispatcher("consume");
    std::lock_guard<std::mutex> lock(broker_->mutex);
    requireOpen();
    auto it = broker_->queues.find(queue);
    if (it == broker_->queues.end()) broker_->channelError(ch_, 404, "NOT_FOUND - no queue '" + queue + "'");
    Queue& q = it->second;
    if (exclusive && !q.consumers.empty()) {
      broker_->channelError(ch_, 403, "ACCESS_REFUSED - queue '" + queue + "' in exclusive use");
    }
    q.consumers.push_back(ConsumerRec{tag, ch_, noAck, std::move(onDelivery), std::move(onCancel)});
    q.everConsumed = true;
    ch_->consumerTags.insert(tag);
    broker_->pumpLocked();
    return tag;
  }

  void cancel(const std::string& tag) override {
    broker_->requireNotDispatcher("cancel");
    std::lock_guard<std::mutex> lock(broker_->mutex);
    requireOpen();
    for (auto& [name, q] : broker_->queues) {
      q.consumers.erase(std::remove_if(q.consumers.begin(), q.consumers.end(),
                                       [&](const ConsumerRec& c) {
                                         return c.tag == tag && c.channel.lock() == ch_;
                                       }),
                        q.consumers.end());
    }
    ch_->consumerTags.erase(tag);
    broker_->autoDeleteLocked();
  }

  void ack(uint64_t tag) override {
    std::lock_guard<std::mutex> lock(broker_->mutex);
    if (ch_->closed) return;
    auto it = ch_->unacked.find(tag);
    if (it == ch_->unacked.end()) {
      broker_->closeChannelLocked(ch_, "406 PRECONDITION_FAILED - unknown delivery tag " + std::to_string(tag));
      return;
    }
    ch_->unacked.erase(it);
    broker_->pumpLocked();
  }

  void reject(uint64_t tag, bool requeue) override {
    std::lock_guard<std::mutex> lock(broker_->mutex);
    if (ch_->closed) return;
    auto it = ch_->unacked.find(tag);
    if (it == ch_->unacked.end()) {
      broker_->closeChannelLocked(ch_, "406 PRECONDITION_FAILED - unknown delivery tag " + std::to_string(tag));
      return;
    }
    Unacked u = std::move(it->second);
    ch_->unacked.erase(it);
    if (requeue) {
      broker_->requeueLocked(std::move(u));
    } else {
      auto q = broker_->queues.find(u.queue);
      if (q != broker_->queues.end()) broker_->deadLetterLocked(q->second, std::move(u.message), "rejected");
    }
    broker_->pumpLocked();
  }

  void publish(const std::string& exchange, const std::string& routingKey, const std::string& body,
               const Properties& properties, bool mandatory, ConfirmCallback onConfirm) override {
    std::lock_guard<std::mutex> lock(broker_->mutex);
    if (ch_->closed) throw AmqpError("channel is closed: " + ch_->closeReason, 0, true);
    if (!exchange.empty() && !broker_->exchanges.count(exchange)) {
      // As RabbitMQ does: the channel closes, and this publish with it,
      // unconfirmed.
      auto cb = std::move(onConfirm);
      const std::string reason = "404 NOT_FOUND - no exchange '" + exchange + "'";
      broker_->post([cb, reason] { cb(ConfirmOutcome::Closed, reason); });
      broker_->closeChannelLocked(ch_, reason);
      return;
    }
    const auto targets = broker_->routeLocked(exchange, routingKey);
    if (broker_->confirmMode == MemoryBroker::ConfirmMode::Nack) {
      broker_->post([cb = std::move(onConfirm)] { cb(ConfirmOutcome::Nack, "message nacked by the broker"); });
      return;
    }
    for (const auto& name : targets) {
      Message m;
      m.body = body;
      m.properties = properties;
      m.exchange = exchange;
      m.routingKey = routingKey;
      broker_->enqueueLocked(broker_->queues.at(name), std::move(m));
    }
    if (broker_->confirmMode == MemoryBroker::ConfirmMode::Drop) {
      ch_->pending.push_back(PendingPublish{std::move(onConfirm)});
    } else {
      const auto outcome = targets.empty() && mandatory ? ConfirmOutcome::Returned : ConfirmOutcome::Ack;
      broker_->post([cb = std::move(onConfirm), outcome] { cb(outcome, ""); });
    }
    broker_->pumpLocked();
  }

  void close() override {
    std::lock_guard<std::mutex> lock(broker_->mutex);
    broker_->closeChannelLocked(ch_, "channel closed");
  }

  bool isOpen() const override {
    std::lock_guard<std::mutex> lock(broker_->mutex);
    return !ch_->closed;
  }

  void onClose(std::function<void(const std::string&)> fn) override {
    std::unique_lock<std::mutex> lock(broker_->mutex);
    if (ch_->closed) {
      const std::string reason = ch_->closeReason;
      lock.unlock();
      fn(reason);
      return;
    }
    ch_->closeCallbacks.push_back(std::move(fn));
  }

 private:
  void requireOpen() {
    if (ch_->closed) throw AmqpError("channel is closed: " + ch_->closeReason, 0, true);
  }

  std::shared_ptr<Impl> broker_;
  std::shared_ptr<MemChannel> ch_;
};

class MemConnectionHandle : public amqp::Connection {
 public:
  MemConnectionHandle(std::shared_ptr<Impl> broker, std::shared_ptr<MemConnection> conn)
      : broker_(std::move(broker)), conn_(std::move(conn)) {}

  std::shared_ptr<amqp::Channel> openChannel() override {
    broker_->requireNotDispatcher("openChannel");
    std::lock_guard<std::mutex> lock(broker_->mutex);
    if (conn_->closed) throw AmqpError("the connection is closed", 0, true);
    auto ch = std::make_shared<MemChannel>();
    ch->broker = broker_;
    ch->connection = conn_.get();
    conn_->channels.push_back(ch);
    return std::make_shared<MemChannelHandle>(broker_, ch);
  }

  void close() override {
    std::lock_guard<std::mutex> lock(broker_->mutex);
    broker_->closeConnectionLocked(conn_.get(), std::nullopt);
  }

  bool isOpen() const override {
    std::lock_guard<std::mutex> lock(broker_->mutex);
    return !conn_->closed;
  }

  void onClose(std::function<void(std::optional<std::string>)> fn) override {
    std::lock_guard<std::mutex> lock(broker_->mutex);
    if (conn_->closed) {
      broker_->post([fn = std::move(fn)] { fn(std::string("the connection was closed")); });
      return;
    }
    conn_->closeCallbacks.push_back(std::move(fn));
  }

 private:
  std::shared_ptr<Impl> broker_;
  std::shared_ptr<MemConnection> conn_;
};

}  // namespace

MemoryBroker::MemoryBroker() : impl_(std::make_shared<Impl>()) { impl_->start(); }

std::shared_ptr<MemoryBroker> MemoryBroker::create() { return std::shared_ptr<MemoryBroker>(new MemoryBroker()); }

MemoryBroker::~MemoryBroker() {
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    auto conns = impl_->connections;
    for (auto& c : conns) impl_->closeConnectionLocked(c.get(), std::nullopt);
  }
  impl_->stop();
}

std::shared_ptr<amqp::Connection> MemoryBroker::connect(const std::string&, int) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  ++impl_->attempts;
  if (impl_->refuse) throw AmqpError("cannot connect to the memory broker: connection refused", 0, false);
  auto conn = std::make_shared<MemConnection>();
  conn->broker = impl_;
  impl_->connections.push_back(conn);
  return std::make_shared<MemConnectionHandle>(impl_, conn);
}

void MemoryBroker::killConnections(const std::string& reason) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  auto conns = impl_->connections;
  for (auto& c : conns) impl_->closeConnectionLocked(c.get(), reason);
}

void MemoryBroker::refuseConnections(bool refuse) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->refuse = refuse;
}

void MemoryBroker::setConfirmMode(ConfirmMode mode) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->confirmMode = mode;
}

size_t MemoryBroker::heldConfirms() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  size_t n = 0;
  for (const auto& conn : impl_->connections) {
    for (const auto& ch : conn->channels) n += ch->closed ? 0 : ch->pending.size();
  }
  return n;
}

size_t MemoryBroker::releaseHeldConfirms(amqp::ConfirmOutcome outcome) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  size_t n = 0;
  for (const auto& conn : impl_->connections) {
    for (const auto& ch : conn->channels) {
      if (ch->closed) continue;
      auto pending = std::move(ch->pending);
      ch->pending.clear();
      n += pending.size();
      impl_->post([pending = std::move(pending), outcome]() mutable {
        for (auto& p : pending) p.callback(outcome, "");
      });
    }
  }
  return n;
}

void MemoryBroker::closeChannelsConsuming(const std::string& queue, const std::string& reason) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  auto it = impl_->queues.find(queue);
  if (it == impl_->queues.end()) return;
  std::vector<std::shared_ptr<MemChannel>> channels;
  for (auto& c : it->second.consumers) {
    if (auto ch = c.channel.lock()) channels.push_back(ch);
  }
  for (auto& ch : channels) impl_->closeChannelLocked(ch, reason);
}

bool MemoryBroker::queueExists(const std::string& name) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->queues.count(name) > 0;
}

bool MemoryBroker::exchangeExists(const std::string& name) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->exchanges.count(name) > 0;
}

size_t MemoryBroker::queueDepth(const std::string& name) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  auto it = impl_->queues.find(name);
  return it == impl_->queues.end() ? 0 : it->second.depth();
}

size_t MemoryBroker::unackedCount(const std::string& name) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  size_t n = 0;
  for (const auto& conn : impl_->connections) {
    for (const auto& ch : conn->channels) {
      for (const auto& [_, u] : ch->unacked) n += u.queue == name ? 1 : 0;
    }
  }
  return n;
}

size_t MemoryBroker::consumerCount(const std::string& name) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  auto it = impl_->queues.find(name);
  return it == impl_->queues.end() ? 0 : it->second.consumers.size();
}

std::optional<FieldTable> MemoryBroker::queueArguments(const std::string& name) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  auto it = impl_->queues.find(name);
  if (it == impl_->queues.end()) return std::nullopt;
  return it->second.arguments;
}

std::vector<Delivery> MemoryBroker::peek(const std::string& name) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  std::vector<Delivery> out;
  auto it = impl_->queues.find(name);
  if (it == impl_->queues.end()) return out;
  for (int p = static_cast<int>(it->second.levels.size()) - 1; p >= 0; --p) {
    for (const auto& m : it->second.levels[p]) {
      Delivery d;
      d.body = m.body;
      d.properties = m.properties;
      d.exchange = m.exchange;
      d.routingKey = m.routingKey;
      d.redelivered = m.redelivered;
      out.push_back(std::move(d));
    }
  }
  return out;
}

std::vector<std::string> MemoryBroker::bindings(const std::string& queue, const std::string& exchange) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  std::vector<std::string> out;
  for (const auto& b : impl_->bindingList) {
    if (b.queue == queue && b.exchange == exchange) out.push_back(b.key);
  }
  return out;
}

size_t MemoryBroker::openConnections() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->connections.size();
}

size_t MemoryBroker::connectAttempts() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->attempts;
}

void MemoryBroker::flush() { impl_->flush(); }

}  // namespace protobus::testing
