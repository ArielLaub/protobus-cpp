// MessageListener: a service's request queue on the bus exchange, with its
// retry ladder.
//
// A request that fails with anything but a HandledError is parked on
// `<Service>.Retry`, whose TTL expiry dead-letters it back to the bus
// exchange under its original routing key; once its retries are spent it is
// published to `<Service>.DLQ`.
#pragma once

#include <optional>
#include <string>
#include <vector>

#include "protobus/base_listener.h"

namespace protobus {

struct RetryOptions {
  // Retry hops before the DLQ. 0 disables retries. Default 3.
  std::optional<int> maxRetries;
  // Delay between hops, the retry queue's x-message-ttl. Default 5000.
  std::optional<int64_t> retryDelayMs;
  // x-message-ttl on the service's own queue. Default: none.
  std::optional<int64_t> messageTtlMs;
};

inline constexpr int kDefaultMaxRetries = 3;
inline constexpr int64_t kDefaultRetryDelayMs = 5000;

struct RetryConfig {
  int maxRetries = kDefaultMaxRetries;
  int64_t retryDelayMs = kDefaultRetryDelayMs;
  std::optional<int64_t> messageTtlMs;
};

class MessageListener : public BaseListener {
 public:
  // maxPriority requires lateAck: an early-ack consumer has no prefetch, so
  // the broker hands it the whole backlog and priority has nothing left to
  // reorder. Throws InvalidPriorityError.
  MessageListener(std::shared_ptr<Connection> connection, bool lateAck = false,
                  std::optional<int> maxConcurrent = std::nullopt, RetryOptions retry = {},
                  std::optional<int64_t> processingTimeoutMs = std::nullopt,
                  std::optional<int> maxPriority = std::nullopt);

  // Bind the queue to each routing key, then declare the retry topology.
  void subscribe(const std::vector<std::string>& topics);
  void subscribe(const std::string& topic) { subscribe(std::vector<std::string>{topic}); }

  std::string getRetryQueueName() const { return retryQueueName_; }
  std::string getDlqName() const { return dlqName_; }
  const RetryConfig& getRetryConfig() const { return retryConfig_; }

  // Builds the caller's reply for a failure that carries none (a processing
  // timeout, say).
  void setErrorReplyBuilder(
      std::function<std::optional<std::string>(const std::string&, const std::exception&)> build) {
    buildErrorReply_ = std::move(build);
  }

 protected:
  std::optional<ConsumeRetryOptions> getRetryOptions() const override;
  void restoreTopology() override;
  const char* listenerName() const override { return "MessageListener"; }

 private:
  void setupRetryQueues();

  RetryConfig retryConfig_;
  std::string dlqName_;
  std::string retryQueueName_;
  std::string retryExchangeName_;
};

}  // namespace protobus
