#include "protobus/message_dispatcher.h"

#include <algorithm>
#include <cctype>
#include <deque>

#include "executor.h"
#include "protobus/config.h"
#include "protobus/errors.h"
#include "protobus/logger.h"
#include "protobus/priority.h"
#include "scheduler.h"
#include "uuid.h"

namespace protobus {

namespace detail {

// One streaming call. Its state is guarded by the registry's mutex.
struct StreamCall {
  std::shared_ptr<StreamRegistry> registry;
  std::string id;
  int64_t idleTimeoutMs = 0;
  std::condition_variable cv;
  std::deque<std::string> chunks;
  int64_t bufferedBytes = 0;
  // Highest x-protobus-seq accepted so far. nullopt before the first chunk,
  // and for peers that send no sequence header.
  std::optional<int64_t> lastSeq;
  bool ended = false;
  std::exception_ptr error;
  bool cancelled = false;
  bool released = false;
  uint64_t idleTimer = 0;
  std::optional<AbortSignal> signal;
  uint64_t signalListener = 0;
};

// The streams of one dispatcher, and the means to cancel them. Shared with
// every ChunkStream, so a stream outliving its dispatcher stays safe.
struct StreamRegistry {
  std::mutex mutex;
  std::map<std::string, std::shared_ptr<StreamCall>> pending;
  // Bytes buffered across every pending stream: bounds the process, not one
  // call.
  int64_t totalBufferedBytes = 0;
  std::weak_ptr<Connection> connection;
  // Publishes the cancellation notice for a stream. Fire-and-forget.
  std::function<void(const std::string& id)> publishCancel;
  // Set by MessageDispatcher::close(): no new stream may register.
  bool closed = false;

  void releaseBytes(int64_t n) { totalBufferedBytes = std::max<int64_t>(0, totalBufferedBytes - n); }

  void clearIdle(StreamCall& call) {
    if (call.idleTimer == 0) return;
    if (auto c = connection.lock()) c->scheduler().cancel(call.idleTimer);
    call.idleTimer = 0;
  }

  void releaseSignal(StreamCall& call) {
    if (call.signal && call.signalListener != 0) call.signal->removeListener(call.signalListener);
    call.signalListener = 0;
  }

  // Everything this call holds, released on any terminal outcome.
  void releaseLocked(StreamCall& call) {
    clearIdle(call);
    releaseSignal(call);
    pending.erase(call.id);
    releaseBytes(call.bufferedBytes);
    call.bufferedBytes = 0;
    call.chunks.clear();
    call.released = true;
  }

  // Idle deadline for the whole call, armed at creation rather than on the
  // first next(): a caller that never iterates must not hold the entry and
  // everything the server sends into it for the life of the process.
  void armIdleLocked(const std::shared_ptr<StreamCall>& call) {
    clearIdle(*call);
    auto c = connection.lock();
    if (!c) return;
    std::weak_ptr<StreamCall> weak = call;
    std::weak_ptr<StreamRegistry> weakRegistry = call->registry;
    call->idleTimer = c->scheduler().schedule(std::chrono::milliseconds(call->idleTimeoutMs), [weak, weakRegistry] {
      auto call = weak.lock();
      auto registry = weakRegistry.lock();
      if (!call || !registry) return;
      std::unique_lock<std::mutex> lock(registry->mutex);
      call->idleTimer = 0;
      if (call->ended) return;
      // The producer is still generating for a caller that stopped
      // listening: tell it to stop. Cancelling is also what returns the
      // buffered bytes to the process-wide allowance.
      registry->failLocked(call,
                           std::make_exception_ptr(StreamTimeoutError("No streaming chunk received within " +
                                                                      std::to_string(call->idleTimeoutMs) + "ms")),
                           lock);
    });
  }

  // End every pending stream with `error`: the iterators throw it on their
  // next call. `final` is for an explicit close, after which there is no
  // channel to send a cancellation notice on, so none is attempted later.
  void failAllLocked(const std::exception_ptr& error, bool final) {
    for (auto& [_, stream] : pending) {
      stream->error = error;
      stream->ended = true;
      clearIdle(*stream);
      // Its bytes leave the process-wide total with it, so a later release
      // cannot subtract them from a total that no longer holds them.
      stream->chunks.clear();
      stream->bufferedBytes = 0;
      if (final) {
        releaseSignal(*stream);
        stream->cancelled = true;
      }
      stream->cv.notify_all();
    }
    pending.clear();
    totalBufferedBytes = 0;
  }

  // The caller's side failed the stream (a buffer limit, a lost chunk): keep
  // the error for the iterator and tell the producer to stop. Once only, and
  // the call leaves `pending`, so whatever the producer sends meanwhile is
  // ignored.
  void failLocked(const std::shared_ptr<StreamCall>& call, std::exception_ptr error,
                  std::unique_lock<std::mutex>& lock) {
    call->error = std::move(error);
    call->ended = true;
    call->cv.notify_all();
    cancelLocked(call, true, lock);
  }

  // Stop the producer and release everything the call holds. `notifyOnly` is
  // for a failure path (idle, backpressure, sequence), which has already
  // recorded the error the iterator is about to raise. Best effort and at
  // most once: a lost notice means the producer runs to completion, the same
  // as never cancelling.
  void cancelLocked(const std::shared_ptr<StreamCall>& call, bool notifyOnly, std::unique_lock<std::mutex>& lock) {
    if (call->cancelled) return;
    call->cancelled = true;
    clearIdle(*call);
    releaseSignal(*call);
    pending.erase(call->id);
    releaseBytes(call->bufferedBytes);
    call->chunks.clear();
    call->bufferedBytes = 0;
    if (!notifyOnly) {
      call->ended = true;
      call->released = true;
      call->cv.notify_all();
    }
    Logger::debug("cancelling stream " + call->id);
    auto publish = publishCancel;
    const std::string id = call->id;
    lock.unlock();
    if (publish) publish(id);
    lock.lock();
  }
};

}  // namespace detail

// ---- ChunkStream -----------------------------------------------------------------

ChunkStream::ChunkStream(std::shared_ptr<detail::StreamCall> call) : call_(std::move(call)) {}

ChunkStream& ChunkStream::operator=(ChunkStream&& other) noexcept {
  if (this != &other) {
    try {
      cancel();
    } catch (...) {
    }
    call_ = std::move(other.call_);
  }
  return *this;
}

ChunkStream::~ChunkStream() {
  try {
    cancel();
  } catch (...) {
  }
}

bool ChunkStream::finished() const {
  if (!call_) return true;
  std::lock_guard<std::mutex> lock(call_->registry->mutex);
  return call_->released || call_->cancelled;
}

void ChunkStream::cancel() {
  if (!call_) return;
  auto registry = call_->registry;
  std::unique_lock<std::mutex> lock(registry->mutex);
  // A stream that already ended has nothing to cancel.
  if (call_->released) return;
  registry->cancelLocked(call_, false, lock);
}

std::optional<std::string> ChunkStream::next() {
  if (!call_) return std::nullopt;
  auto registry = call_->registry;
  std::unique_lock<std::mutex> lock(registry->mutex);
  for (;;) {
    if (call_->error) {
      if (!call_->released) registry->releaseLocked(*call_);
      std::rethrow_exception(call_->error);
    }
    if (!call_->chunks.empty()) {
      std::string value = std::move(call_->chunks.front());
      call_->chunks.pop_front();
      call_->bufferedBytes -= static_cast<int64_t>(value.size());
      registry->releaseBytes(static_cast<int64_t>(value.size()));
      // Progress on the consuming side restarts the idle deadline too, so
      // "idle" means the call is stalled, not merely slow at one end.
      registry->armIdleLocked(call_);
      return value;
    }
    if (call_->ended) {
      if (!call_->released) registry->releaseLocked(*call_);
      return std::nullopt;
    }
    call_->cv.wait(lock);
  }
}

// ---- MessageDispatcher -------------------------------------------------------------

namespace {

// AMQP carries message-id as a shortstr: one length byte.
constexpr size_t kMaxMessageIdBytes = 255;

std::optional<std::string> validateMessageId(const std::optional<std::string>& messageId) {
  if (!messageId) return std::nullopt;
  const bool blank = std::all_of(messageId->begin(), messageId->end(),
                                 [](unsigned char c) { return std::isspace(c); });
  // Blank is refused rather than treated as absent: an id derived from a
  // field that turned out empty would give every attempt a different
  // identity, which is the exact failure this option exists to prevent.
  if (blank) {
    throw InvalidMessageIdError("messageId must be a non-empty string, got \"" + *messageId +
                                "\". Leave it unset to have one generated.");
  }
  if (messageId->size() > kMaxMessageIdBytes) {
    throw InvalidMessageIdError("messageId is " + std::to_string(messageId->size()) +
                                " bytes; AMQP carries message-id as a shortstr, so it must be at most " +
                                std::to_string(kMaxMessageIdBytes) +
                                ". Hash a long key rather than concatenating it.");
  }
  return messageId;
}

std::optional<int64_t> parseSeqHeader(const amqp::FieldTable* headers) {
  if (headers == nullptr) return std::nullopt;
  auto it = headers->find(Config::HEADER_SEQ);
  if (it == headers->end()) return std::nullopt;
  auto v = it->second.asInt();
  if (!v || *v < 0) return std::nullopt;
  return v;
}

bool parseFinalHeader(const amqp::FieldTable* headers) {
  if (headers == nullptr) return false;
  auto it = headers->find(Config::HEADER_FINAL);
  if (it == headers->end()) return false;
  return it->second.asBool().value_or(false);
}

}  // namespace

struct MessageDispatcher::PendingCall {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  std::string reply;
  std::exception_ptr error;
  // The deadline timer. Guarded by the dispatcher's mutex_, under which it is
  // armed in the same step that registers the call: whoever takes the call
  // out of callbacks_ also takes the timer, and none can miss it.
  uint64_t timer = 0;
  // The broker confirmed the request: a timeout after this is a missing
  // reply, not a missing request.
  std::atomic<bool> confirmed{false};

  void resolve(std::string value) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (done) return;
      done = true;
      reply = std::move(value);
    }
    cv.notify_all();
  }
  void fail(std::exception_ptr e) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (done) return;
      done = true;
      error = std::move(e);
    }
    cv.notify_all();
  }
  std::string wait() {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&] { return done; });
    if (error) std::rethrow_exception(error);
    return reply;
  }
};

MessageDispatcher::MessageDispatcher(std::shared_ptr<Connection> connection)
    : connection_(std::move(connection)),
      callbackListener_(std::make_shared<CallbackListener>(connection_)),
      streams_(std::make_shared<detail::StreamRegistry>()) {
  streams_->connection = connection_;
}

MessageDispatcher::~MessageDispatcher() {
  if (detachRestorer_) detachRestorer_();
  if (disconnectedListener_) connection_->removeListener(*disconnectedListener_);
  std::lock_guard<std::mutex> lock(streams_->mutex);
  streams_->publishCancel = nullptr;
}

bool MessageDispatcher::isInitialized() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return initialized_;
}

std::string MessageDispatcher::callbackQueue() const { return callbackListener_->callbackQueue(); }

void MessageDispatcher::init() {
  if (isInitialized()) return;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = false;
  }
  {
    std::lock_guard<std::mutex> lock(streams_->mutex);
    streams_->closed = false;
  }
  std::weak_ptr<MessageDispatcher> weak = weak_from_this();
  disconnectedListener_ = connection_->onDisconnected([weak] {
    if (auto self = weak.lock()) self->onDisconnected();
  });
  // The channel is restored under the connection's coordination, so a caller
  // reacting to a reconnection by publishing finds one waiting.
  detachRestorer_ = connection_->registerRestorer([weak](uint64_t) {
    if (auto self = weak.lock()) self->restore();
  });
  {
    std::lock_guard<std::mutex> lock(streams_->mutex);
    // Called from timers and from abort listeners: published from a worker,
    // where waiting on the transport is allowed.
    streams_->publishCancel = [weak](const std::string& id) {
      auto self = weak.lock();
      if (!self) return;
      self->connection_->executor().post([weak, id] {
        if (auto s = weak.lock()) s->publishCancel(id);
      });
    };
  }

  auto ch = connection_->openChannel();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    channel_ = ch;
  }
  declareCoreExchanges();
  callbackListener_->init(
      [weak](const std::string& content, const std::string& correlationId,
             MessageHandlerContext& context) -> MessageHandlerResult {
        if (auto self = weak.lock()) self->onResult(content, correlationId, context.headers);
        return std::monostate{};
      });
  callbackListener_->start();
  std::lock_guard<std::mutex> lock(mutex_);
  initialized_ = true;
}

// Best effort: declaring the exchanges this process publishes to means a
// caller that starts before any service gets UnroutableError rather than a
// channel closed by a 404. A deployment whose credentials cannot declare them
// keeps working when they already exist.
void MessageDispatcher::declareCoreExchanges() {
  try {
    auto ch = connection_->openChannel();
    try {
      connection_->declareExchange(ch, Config::busExchangeName(), "topic");
      connection_->declareExchange(ch, Config::cancelExchangeName(), "fanout");
    } catch (const std::exception& e) {
      Logger::debug(std::string("MessageDispatcher: could not declare the core exchanges: ") + e.what());
    }
    connection_->closeChannel(ch);
  } catch (const std::exception& e) {
    Logger::debug(std::string("MessageDispatcher: could not declare the core exchanges: ") + e.what());
  }
}

std::shared_ptr<amqp::Channel> MessageDispatcher::publishChannel() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (channel_ && channel_->isOpen()) return channel_;
  }
  // A channel lost on a live connection (closed by a broker error) is
  // replaced, rather than failing every later publish.
  if (!connection_->isReady()) {
    std::lock_guard<std::mutex> lock(mutex_);
    return channel_;
  }
  auto ch = connection_->openChannel();
  std::lock_guard<std::mutex> lock(mutex_);
  if (!channel_ || !channel_->isOpen()) channel_ = ch;
  return channel_;
}

// Fire-and-forget: the caller has already stopped waiting, and a lost notice
// only means the producer runs to completion.
void MessageDispatcher::publishCancel(const std::string& id) {
  std::shared_ptr<amqp::Channel> ch;
  try {
    ch = publishChannel();
  } catch (...) {
  }
  if (!ch) return;
  PublishOptions p;
  p.properties.correlationId = id;
  p.properties.contentType = "application/octet-stream";
  connection_->publishAsync(ch, Config::cancelExchangeName(), "", std::string(), std::move(p),
                            [id](std::exception_ptr err) {
                              if (!err) return;
                              try {
                                std::rethrow_exception(err);
                              } catch (const std::exception& e) {
                                Logger::debug("failed to publish cancel for stream " + id + ": " + e.what());
                              }
                            });
}

void MessageDispatcher::onDisconnected() {
  Logger::debug("MessageDispatcher: connection lost, rejecting pending callbacks");
  {
    std::lock_guard<std::mutex> lock(mutex_);
    channel_.reset();
  }
  failPending(std::make_exception_ptr(DisconnectedError()), false);
}

// Every pending call and stream ends with `error`, timers cancelled and
// buffers released. Each call is completed by exactly one party: whoever
// takes it out of callbacks_.
void MessageDispatcher::failPending(const std::exception_ptr& error, bool final) {
  std::map<std::string, std::shared_ptr<PendingCall>> calls;
  std::vector<uint64_t> timers;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    calls.swap(callbacks_);
    for (auto& [_, call] : calls) {
      timers.push_back(call->timer);
      call->timer = 0;
    }
  }
  for (auto t : timers) connection_->scheduler().cancel(t);
  for (auto& [_, call] : calls) call->fail(error);
  std::lock_guard<std::mutex> lock(streams_->mutex);
  streams_->failAllLocked(error, final);
}

std::shared_ptr<MessageDispatcher::PendingCall> MessageDispatcher::takeCall(const std::string& id,
                                                                             const PendingCall* expected) {
  std::shared_ptr<PendingCall> call;
  uint64_t timer = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = callbacks_.find(id);
    if (it == callbacks_.end() || (expected != nullptr && it->second.get() != expected)) return nullptr;
    call = std::move(it->second);
    callbacks_.erase(it);
    timer = call->timer;
    call->timer = 0;
  }
  if (timer != 0) connection_->scheduler().cancel(timer);
  return call;
}

// A failure propagates: a dispatcher with no channel cannot publish, so the
// generation should be retried rather than announced.
void MessageDispatcher::restore() {
  if (!isInitialized()) return;
  Logger::info("MessageDispatcher: reconnected, re-initializing channel");
  auto ch = connection_->openChannel();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    channel_ = ch;
  }
  declareCoreExchanges();
  Logger::info("MessageDispatcher: successfully re-initialized after reconnection");
}

void MessageDispatcher::onResult(const std::string& content, const std::string& id,
                                 const amqp::FieldTable* headers) {
  {
    std::unique_lock<std::mutex> lock(streams_->mutex);
    auto it = streams_->pending.find(id);
    if (it != streams_->pending.end()) {
      auto stream = it->second;
      const bool isFinal = parseFinalHeader(headers);

      // Sequence validation catches a lost chunk, which would otherwise
      // present as a silently truncated stream. An absent header disables
      // it: a violation must not be inferred from missing information.
      if (auto seq = parseSeqHeader(headers)) {
        const int64_t expected = stream->lastSeq ? *stream->lastSeq + 1 : 0;
        if (*seq < expected) {
          // Already seen: a redelivery, not new data.
          Logger::debug("stream " + id + ": dropping duplicate chunk seq=" + std::to_string(*seq) +
                        " (expected " + std::to_string(expected) + ")");
          if (isFinal) stream->ended = true;
          stream->cv.notify_all();
          return;
        }
        if (*seq > expected) {
          streams_->failLocked(stream,
                               std::make_exception_ptr(StreamSequenceError(
                                   "stream " + id + " lost at least one chunk: got seq=" + std::to_string(*seq) +
                                   ", expected " + std::to_string(expected))),
                               lock);
          return;
        }
        stream->lastSeq = seq;
      }

      // An empty final-only body is "end of stream, no extra data".
      if (!content.empty()) {
        // Bound the buffer: a server faster than its caller would otherwise
        // grow it until the process died. Failing the stream is
        // recoverable; running out of memory is not.
        const int64_t maxChunks = Config::streamMaxBufferedChunks();
        const int64_t maxBytes = Config::streamMaxBufferedBytes();
        const int64_t maxTotal = Config::streamMaxTotalBufferedBytes();
        const int64_t size = static_cast<int64_t>(content.size());
        const int64_t wouldBeBytes = stream->bufferedBytes + size;
        const int64_t wouldBeTotal = streams_->totalBufferedBytes + size;
        if (static_cast<int64_t>(stream->chunks.size()) + 1 > maxChunks || wouldBeBytes > maxBytes ||
            wouldBeTotal > maxTotal) {
          streams_->failLocked(
              stream,
              std::make_exception_ptr(StreamBackpressureError(
                  "stream " + id + " exceeded a buffer limit (" + std::to_string(stream->chunks.size() + 1) +
                  " chunks / " + std::to_string(wouldBeBytes) + " bytes for this call, " +
                  std::to_string(wouldBeTotal) + " bytes across all calls; limits are " +
                  std::to_string(maxChunks) + " chunks / " + std::to_string(maxBytes) + " bytes / " +
                  std::to_string(maxTotal) + " bytes total) - the consumer is not keeping up with the producer")),
              lock);
          return;
        }
        stream->chunks.push_back(content);
        stream->bufferedBytes = wouldBeBytes;
        streams_->totalBufferedBytes = wouldBeTotal;
        streams_->armIdleLocked(stream);
      }
      if (isFinal) stream->ended = true;
      stream->cv.notify_all();
      return;
    }
  }

  if (auto call = takeCall(id)) call->resolve(content);
}

// A reconnection in progress is waited through rather than failed on: the
// channel is being replaced and will be there shortly. Anything else with no
// connection is a caller error.
void MessageDispatcher::awaitPublishable() {
  if (!connection_->isConnected() && !connection_->isReconnecting()) throw NotConnectedError();
  connection_->whenReady();
}

std::string MessageDispatcher::publish(const std::string& content, const std::string& routingKey,
                                       const CallOptions& options) {
  const auto priority = validatePriority(options.priority);
  const auto callerMessageId = validateMessageId(options.messageId);
  awaitPublishable();

  const bool rpc = options.rpc;
  const std::string id = detail::randomUuid();
  PublishOptions p;
  p.properties.contentType = "application/octet-stream";
  p.properties.correlationId = id;
  if (rpc) p.properties.replyTo = callbackListener_->callbackQueue();
  p.properties.deliveryMode = 2;
  // A request that routes nowhere means no service is bound to the key: a
  // definite error, worth learning at once as UnroutableError rather than
  // after a full timeout. Not for events, whose having no subscriber is
  // normal.
  p.mandatory = rpc;
  // Set only when asked for, so a publish with neither carries neither.
  if (priority) p.properties.priority = static_cast<uint8_t>(*priority);
  if (callerMessageId) p.properties.messageId = *callerMessageId;

  if (!rpc) {
    connection_->publish(publishChannel(), Config::busExchangeName(), routingKey, content, std::move(p));
    return std::string();
  }

  const int64_t limit = options.timeoutMs.value_or(Config::rpcCallTimeoutMs());

  // The deadline covers the broker confirm as well as the reply: the request
  // is published asynchronously, and the call ends at the first of its reply,
  // a definite publish failure, the deadline, a disconnect or a close. The
  // call is registered BEFORE publishing, since a fast service can reply
  // while the confirm is still in flight, and a reply finding no entry is
  // dropped.
  auto call = std::make_shared<PendingCall>();
  std::weak_ptr<MessageDispatcher> weak = weak_from_this();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) throw NotConnectedError("the message dispatcher is closed");
    callbacks_[id] = call;
    call->timer = connection_->scheduler().schedule(
        std::chrono::milliseconds(limit), [weak, id, call, routingKey, limit] {
          auto self = weak.lock();
          if (!self || !self->takeCall(id, call.get())) return;
          std::string message =
              "no reply for " + routingKey + " (correlationId " + id + ") within " + std::to_string(limit) + "ms";
          // Honest about what is known: without a confirm the request may or
          // may not have reached the broker.
          if (!call->confirmed.load()) message += "; the broker had not confirmed the request either";
          call->fail(std::make_exception_ptr(RpcTimeoutError(message)));
        });
  }

  std::shared_ptr<amqp::Channel> ch;
  try {
    ch = publishChannel();
  } catch (...) {
    if (auto taken = takeCall(id, call.get())) taken->fail(std::current_exception());
    return call->wait();
  }
  // A publish failure is the more specific answer, when it comes first: a
  // nack or a return is definite, a closed channel or a confirm timeout
  // ambiguous. One arriving after the reply or the deadline changes nothing.
  connection_->publishAsync(ch, Config::busExchangeName(), routingKey, content, std::move(p),
                            [weak, id, call](std::exception_ptr err) {
                              if (!err) {
                                call->confirmed = true;
                                return;
                              }
                              auto self = weak.lock();
                              if (!self || !self->takeCall(id, call.get())) return;
                              call->fail(std::move(err));
                            });
  return call->wait();
}

ChunkStream MessageDispatcher::publishStreaming(const std::string& content, const std::string& routingKey,
                                                const StreamOptions& options) {
  if (!connection_->isConnected() && !connection_->isReconnecting()) throw NotConnectedError();

  auto call = std::make_shared<detail::StreamCall>();
  call->registry = streams_;
  call->id = detail::randomUuid();
  call->idleTimeoutMs = options.idleTimeoutMs.value_or(Config::streamIdleTimeoutMs());
  call->signal = options.signal;

  const bool abortedBeforeStart = options.signal && options.signal->aborted();
  {
    std::unique_lock<std::mutex> lock(streams_->mutex);
    if (streams_->closed) throw NotConnectedError("the message dispatcher is closed");
    if (abortedBeforeStart) {
      // Aborted before it began: nothing to send and nothing to wait for.
      call->ended = true;
      call->released = true;
      call->cancelled = true;
      return ChunkStream(call);
    }
    streams_->pending[call->id] = call;
    streams_->armIdleLocked(call);
  }
  if (options.signal) {
    std::weak_ptr<detail::StreamCall> weakCall = call;
    std::weak_ptr<detail::StreamRegistry> weakRegistry = streams_;
    const uint64_t listener = options.signal->addListener([weakCall, weakRegistry] {
      auto c = weakCall.lock();
      auto r = weakRegistry.lock();
      if (!c || !r) return;
      std::unique_lock<std::mutex> lock(r->mutex);
      if (c->released) return;
      r->cancelLocked(c, false, lock);
    });
    std::lock_guard<std::mutex> lock(streams_->mutex);
    call->signalListener = listener;
    if (call->cancelled) streams_->releaseSignal(*call);
  }

  PublishOptions p;
  p.properties.contentType = "application/octet-stream";
  p.properties.correlationId = call->id;
  p.properties.replyTo = callbackListener_->callbackQueue();
  p.properties.deliveryMode = 2;
  try {
    // The channel is read after readiness: a reconnection replaces it.
    connection_->whenReady();
    connection_->publish(publishChannel(), Config::busExchangeName(), routingKey, content, std::move(p));
  } catch (...) {
    std::lock_guard<std::mutex> lock(streams_->mutex);
    call->error = std::current_exception();
    call->ended = true;
    call->cv.notify_all();
  }
  return ChunkStream(call);
}

void MessageDispatcher::close() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
  }
  {
    std::lock_guard<std::mutex> lock(streams_->mutex);
    streams_->closed = true;
  }
  // Nothing more can arrive for what is pending: the reply queue goes with
  // this close. End it all now rather than at each call's own deadline.
  failPending(std::make_exception_ptr(DisconnectedError("the message dispatcher was closed with the call pending")),
              true);
  if (disconnectedListener_) {
    connection_->removeListener(*disconnectedListener_);
    disconnectedListener_.reset();
  }
  if (detachRestorer_) {
    detachRestorer_();
    detachRestorer_ = nullptr;
  }
  if (callbackListener_->isInitialized()) callbackListener_->close();
  std::lock_guard<std::mutex> lock(mutex_);
  initialized_ = false;
}

}  // namespace protobus
