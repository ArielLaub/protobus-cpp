// MessageDispatcher: the client side of RPC. It publishes requests on the
// bus exchange and matches the replies arriving on this process's callback
// queue to the calls awaiting them.
#pragma once

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "protobus/call_options.h"
#include "protobus/callback_listener.h"
#include "protobus/connection.h"
#include "protobus/stream.h"

namespace protobus {

namespace detail {
struct StreamRegistry;
}

class MessageDispatcher : public std::enable_shared_from_this<MessageDispatcher> {
 public:
  explicit MessageDispatcher(std::shared_ptr<Connection> connection);
  ~MessageDispatcher();

  bool isInitialized() const;
  void init();

  // Publish a request and, unless options.rpc is false, wait for its reply
  // and return the raw reply body. The deadline (options.timeoutMs) starts
  // once the connection is ready to publish and bounds the whole call, the
  // broker confirm included. Whichever comes first ends it: the reply; a
  // failed publish, as its PublishError; the deadline, as RpcTimeoutError
  // (the request may or may not have been delivered); a disconnect, or
  // close(), as DisconnectedError.
  std::string publish(const std::string& content, const std::string& routingKey, const CallOptions& options = {});

  // Publish a request expecting a streaming reply. The request is published
  // before this returns; a failure to publish surfaces from the stream's
  // first next().
  ChunkStream publishStreaming(const std::string& content, const std::string& routingKey,
                               const StreamOptions& options = {});

  // Stop: every pending call and stream fails at once with
  // DisconnectedError, and later calls throw NotConnectedError until init().
  // Safe to call more than once.
  void close();

  // The reply queue's name.
  std::string callbackQueue() const;

 private:
  struct PendingCall;

  void onResult(const std::string& content, const std::string& id, const amqp::FieldTable* headers);
  void onDisconnected();
  void failPending(const std::exception_ptr& error, bool final);
  std::shared_ptr<PendingCall> takeCall(const std::string& id, const PendingCall* expected = nullptr);
  void publishCancel(const std::string& id);
  void restore();
  void awaitPublishable();
  void declareCoreExchanges();
  std::shared_ptr<amqp::Channel> publishChannel();

  std::shared_ptr<Connection> connection_;
  std::shared_ptr<CallbackListener> callbackListener_;
  std::shared_ptr<detail::StreamRegistry> streams_;

  mutable std::mutex mutex_;
  std::shared_ptr<amqp::Channel> channel_;
  bool initialized_ = false;
  bool closed_ = false;
  std::map<std::string, std::shared_ptr<PendingCall>> callbacks_;
  std::function<void()> detachRestorer_;
  std::optional<Connection::ListenerId> disconnectedListener_;
};

}  // namespace protobus
