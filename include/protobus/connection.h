// Connection: the broker connection every protobus component shares.
//
// It owns the AMQP connection and its lifecycle: automatic reconnection with
// exponential backoff, coordinated restoration of every component's topology
// before the connection reports itself ready again, confirmed publishing, and
// the consume loop that runs a handler and settles its delivery (reply, ack,
// retry, dead-letter).
//
// Every blocking method here may be called from any thread except a transport
// callback. Handlers run on worker threads of the connection's own.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <variant>
#include <vector>

#include "protobus/abort.h"
#include "protobus/generator.h"
#include "protobus/transport.h"

namespace protobus {

namespace detail {
class Executor;
class Scheduler;
}  // namespace detail

struct ReconnectionOptions {
  int maxRetries = 10;  // consecutive failed attempts; 0 retries forever
  int64_t initialDelayMs = 1000;
  int64_t maxDelayMs = 30000;
  double backoffMultiplier = 2;
};

// Restores one component's topology after the socket comes back: its channel,
// queues, bindings and consumers. Receives the generation it is restoring.
// Throws to fail the reconnection attempt.
using Restorer = std::function<void(uint64_t generation)>;

// Extra context handed to a message handler.
//
// `signal` fires when the processing timeout elapses or, for a streaming
// reply, when the caller cancels. C++ cannot preempt a running function, so a
// handler doing long work should watch it.
struct MessageHandlerContext {
  AbortSignal signal;
  // The routing key the broker actually delivered on.
  std::string routingKey;
  // Stable across every redelivery and retry hop of one logical message,
  // which is what makes deduplication possible. Empty only for a message
  // published by something that did not set it.
  std::string messageId;
  // The broker has delivered this message before.
  bool redelivered = false;
  const amqp::FieldTable* headers = nullptr;
  // What the handler's abort listeners may refer to. The connection holds it
  // until the handler has returned AND any listener its processing timeout
  // started has finished. MessageService sets it to the service.
  std::shared_ptr<void> keepAlive;
};

// What a handler returns: nothing (no reply), one encoded reply, or a stream
// of encoded chunks, each published with x-protobus-final=false and the last
// with x-protobus-final=true.
using MessageHandlerResult = std::variant<std::monostate, std::string, Generator<std::string>>;

using MessageHandler = std::function<MessageHandlerResult(const std::string& content, const std::string& correlationId,
                                                          MessageHandlerContext& context)>;

// An error carrying the reply its caller should receive on a terminal path
// (dead-lettered, or rejected without retry). MessageService throws one for
// an unhandled handler error, pre-encoded and sanitised; the connection layer
// unwraps it, settling on the original error and replying with `reply`.
class ErrorWithReply : public std::exception {
 public:
  ErrorWithReply(std::exception_ptr cause, std::string reply) : cause_(std::move(cause)), reply_(std::move(reply)) {}
  const char* what() const noexcept override { return "protobus: handler failed"; }
  const std::exception_ptr& cause() const noexcept { return cause_; }
  const std::string& reply() const noexcept { return reply_; }

 private:
  std::exception_ptr cause_;
  std::string reply_;
};

struct ConsumeOptions {
  std::string consumerTag;
  bool noAck = false;
  bool exclusive = false;
  // Handle deliveries one at a time, in arrival order, rather than in
  // parallel up to the prefetch. Replies use it: a stream's chunks must be
  // seen in the order the broker delivered them.
  bool ordered = false;
  // Builds the caller's reply for a failure that carries none, such as a
  // processing timeout. Returning nullopt leaves the caller to its own
  // timeout.
  std::function<std::optional<std::string>(const std::string& content, const std::exception& error)> buildErrorReply;
  // Called when the broker cancels the consumer (basic.cancel: its queue was
  // deleted, say), which leaves the channel and connection open. Runs on the
  // transport's thread: must not block.
  std::function<void()> onCancelled;
};

struct ConsumeRetryOptions {
  int maxRetries = 0;
  std::string retryQueueName;
  // The topic exchange the retry queue is bound to with `#`. A retry is
  // published here under the original routing key, which is what lets the
  // post-TTL dead-letter hop route it back to the service's queue.
  std::string retryExchangeName;
  std::string dlqName;
  std::function<bool(const std::exception&)> isHandledError;
};

struct ExchangeOptions {
  bool durable = true;
  bool autoDelete = false;
  bool internal = false;
  amqp::FieldTable arguments;
};

struct QueueOptions {
  bool durable = true;
  bool exclusive = false;
  bool autoDelete = false;
  amqp::FieldTable arguments;
};

struct PublishOptions {
  amqp::Properties properties;
  bool mandatory = false;
};

class Connection : public std::enable_shared_from_this<Connection> {
 public:
  explicit Connection(std::shared_ptr<amqp::Transport> transport = nullptr);
  ~Connection();

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  // ---- state ---------------------------------------------------------------------

  bool isConnected() const;
  bool isReconnecting() const;
  // True when the socket is up AND every restorer has finished. Between the
  // two, channels are gone and queues not yet redeclared.
  bool isReady() const;

  // ---- lifecycle -----------------------------------------------------------------

  // Connect. Throws AlreadyConnectedError when connected, and the transport's
  // error when the broker cannot be reached.
  void connect(const std::string& url, ReconnectionOptions options = {});
  // Close deliberately. Waiters on readiness fail with NotReadyError, and the
  // onDisconnected listeners run, so pending calls and streams fail at once.
  void disconnect();

  // Register topology this connection must put back before it reports itself
  // reconnected. Restorers run in registration order; one that throws fails
  // the whole attempt. Returns a function that unregisters it.
  std::function<void()> registerRestorer(Restorer restore);

  // Block until the connection carries traffic again. Throws NotReadyError
  // when it is closed or gives up first, or after `timeoutMs` (default
  // Config::connectionReadyTimeoutMs()).
  void whenReady(std::optional<int64_t> timeoutMs = std::nullopt);

  // Stop producing a streaming reply the caller has abandoned: abort the
  // handler's signal and publish nothing more it yields. Returns whether a
  // matching in-flight delivery was found.
  bool cancelStream(const std::string& correlationId);

  // How many messages are being handled, counting handlers still running
  // after their delivery was settled on a processing timeout.
  size_t inFlightDeliveries() const;
  // Wait for in-flight work to finish, up to `timeoutMs`. Returns false when
  // the deadline passed with work still running.
  bool drainInFlight(int64_t timeoutMs);

  // ---- events ----------------------------------------------------------------------

  using ListenerId = uint64_t;
  ListenerId onReconnecting(std::function<void(int attempt, int64_t delayMs)> fn);
  ListenerId onReconnected(std::function<void()> fn);
  // The connection went down: lost (a reconnection is scheduled next) or
  // closed by disconnect() (nothing follows). Runs on the
  // transport's thread, or on disconnect()'s caller: must not block.
  ListenerId onDisconnected(std::function<void()> fn);
  ListenerId onError(std::function<void(const std::exception&)> fn);
  void removeListener(ListenerId id);

  // ---- channel operations -----------------------------------------------------------

  std::shared_ptr<amqp::Channel> openChannel();
  void closeChannel(const std::shared_ptr<amqp::Channel>& channel);
  void declareExchange(const std::shared_ptr<amqp::Channel>& channel, const std::string& exchange,
                       const std::string& type, const ExchangeOptions& options = {});
  std::string declareQueue(const std::shared_ptr<amqp::Channel>& channel, const std::string& queueName,
                           const QueueOptions& options);
  void bindQueue(const std::shared_ptr<amqp::Channel>& channel, const std::string& queue,
                 const std::string& exchange, const std::string& routingKey, const amqp::FieldTable& args = {});
  void unbindQueue(const std::shared_ptr<amqp::Channel>& channel, const std::string& queue,
                   const std::string& exchange, const std::string& routingKey, const amqp::FieldTable& args = {});
  void deleteQueue(const std::shared_ptr<amqp::Channel>& channel, const std::string& queueName);
  void purgeQueue(const std::shared_ptr<amqp::Channel>& channel, const std::string& queueName);
  void ack(const std::shared_ptr<amqp::Channel>& channel, const amqp::Delivery& message);
  void reject(const std::shared_ptr<amqp::Channel>& channel, const amqp::Delivery& message, bool requeue);
  void cancel(const std::shared_ptr<amqp::Channel>& channel, const std::string& consumerTag);

  // Consume `queueName` with `handler`.
  //
  // An early-ack consumer (lateAck false) acknowledges on delivery and never
  // retries. A late-ack consumer settles after the handler: it replies, then
  // acks; on a failure it retries through `retry`, dead-letters once the
  // retries are spent, and answers the caller on every terminal path.
  void consume(const std::shared_ptr<amqp::Channel>& channel, const std::string& queueName, MessageHandler handler,
               const ConsumeOptions& options, bool lateAck, std::optional<ConsumeRetryOptions> retry = std::nullopt,
               std::optional<int64_t> processingTimeoutMs = std::nullopt);

  // Publish and return only once RabbitMQ has confirmed the message:
  // positively (basic.ack) and, for a mandatory publish, routed. Everything
  // else throws a PublishError. PublishConfirmTimeoutError and
  // ChannelClosedError are AMBIGUOUS: the broker may have stored the message.
  // Every publish carries a messageId (a UUID unless one is set) for
  // consumers to deduplicate on. Returns the messageId.
  std::string publish(const std::shared_ptr<amqp::Channel>& channel, const std::string& exchange,
                      const std::string& routingKey, const std::string& content, PublishOptions options);
  // publish() through the default exchange to a queue by name.
  std::string publishToQueue(const std::shared_ptr<amqp::Channel>& channel, const std::string& queueName,
                             const std::string& content, PublishOptions options);
  // publish() without waiting: `done` receives null or the failure, on
  // whichever thread settles it, often the transport's: it must not block.
  // Safe to call from a transport callback.
  void publishAsync(const std::shared_ptr<amqp::Channel>& channel, const std::string& exchange,
                    const std::string& routingKey, const std::string& content, PublishOptions options,
                    std::function<void(std::exception_ptr)> done);

  // Internal: the connection's worker pool and timers, shared with the
  // components built on it.
  detail::Executor& executor();
  detail::Scheduler& scheduler();

 private:
  struct PublishState;
  struct DeliveryEntry;
  struct Delivery;

  std::shared_ptr<amqp::Connection> doConnect();
  std::shared_ptr<amqp::Connection> connectOnce();
  void onHandleClosed(const amqp::Connection* handle, std::optional<std::string> reason);
  void scheduleReconnect();
  void reconnectAttempt();
  void runRestorers(uint64_t generation);
  void discardGeneration();
  void markReady();
  void markNotReady();
  void abandonReady(const std::string& message);

  void emitReconnecting(int attempt, int64_t delayMs);
  void emitReconnected();
  void emitDisconnected();
  void emitError(const std::exception& error);

  std::shared_ptr<PublishState> publishStateFor(const std::shared_ptr<amqp::Channel>& channel);
  void confirmedPublish(const std::shared_ptr<amqp::Channel>& channel, const std::string& exchange,
                        const std::string& routingKey, const std::string& content, PublishOptions options,
                        std::function<void(std::exception_ptr)> done);

  void handleDelivery(std::shared_ptr<Delivery> d);
  void settle(const std::shared_ptr<Delivery>& d, MessageHandlerResult result);
  void settleError(const std::shared_ptr<Delivery>& d, std::exception_ptr error);
  void finishDelivery(const std::shared_ptr<Delivery>& d);
  void publishStreamReply(const std::shared_ptr<amqp::Channel>& channel, const std::string& replyTo,
                          const std::string& correlationId, Generator<std::string>& chunks,
                          const std::function<bool()>& isCancelled);

  void deliveryStarted();
  void deliveryFinished();
  void handlerStarted();
  void handlerFinished();

  std::shared_ptr<amqp::Transport> transport_;
  std::shared_ptr<detail::Executor> executor_;
  std::shared_ptr<detail::Scheduler> scheduler_;

  mutable std::mutex mutex_;
  std::condition_variable readyCv_;
  std::string url_;
  ReconnectionOptions reconnection_;
  std::shared_ptr<amqp::Connection> handle_;
  bool connected_ = false;
  bool reconnecting_ = false;
  bool ready_ = false;
  bool manualDisconnect_ = false;
  uint64_t generation_ = 0;
  int reconnectAttempts_ = 0;
  uint64_t reconnectTimer_ = 0;
  uint64_t abandonCount_ = 0;
  std::string abandonMessage_;

  std::mutex connectMutex_;  // single-flight connect

  std::mutex restorerMutex_;
  uint64_t nextRestorerId_ = 1;
  std::vector<std::pair<uint64_t, Restorer>> restorers_;

  std::mutex listenerMutex_;
  ListenerId nextListenerId_ = 1;
  std::map<ListenerId, std::function<void(int, int64_t)>> reconnectingListeners_;
  std::map<ListenerId, std::function<void()>> reconnectedListeners_;
  std::map<ListenerId, std::function<void()>> disconnectedListeners_;
  std::map<ListenerId, std::function<void(const std::exception&)>> errorListeners_;

  std::mutex publishMutex_;
  std::map<const amqp::Channel*, std::shared_ptr<PublishState>> publishStates_;

  std::mutex deliveryMutex_;
  std::map<std::string, std::set<std::shared_ptr<DeliveryEntry>>> activeDeliveries_;

  mutable std::mutex drainMutex_;
  std::condition_variable drainCv_;
  size_t inFlightDeliveries_ = 0;
  size_t runningHandlers_ = 0;
};

}  // namespace protobus
