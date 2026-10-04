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
  // and return the raw reply body. A publish that fails throws its
  // PublishError (the more specific answer) even when the deadline has also
  // passed; a disconnect while waiting throws DisconnectedError; no reply in
  // time throws RpcTimeoutError.
  std::string publish(const std::string& content, const std::string& routingKey, const CallOptions& options = {});

  // Publish a request expecting a streaming reply. The request is published
  // before this returns; a failure to publish surfaces from the stream's
  // first next().
  ChunkStream publishStreaming(const std::string& content, const std::string& routingKey,
                               const StreamOptions& options = {});

  void close();

  // The reply queue's name.
  std::string callbackQueue() const;

 private:
  struct PendingCall;

  void onResult(const std::string& content, const std::string& id, const amqp::FieldTable* headers);
  void onDisconnected();
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
  std::map<std::string, std::shared_ptr<PendingCall>> callbacks_;
  std::function<void()> detachRestorer_;
  std::optional<Connection::ListenerId> disconnectedListener_;
};

}  // namespace protobus
