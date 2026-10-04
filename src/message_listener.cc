#include "protobus/message_listener.h"

#include "protobus/config.h"
#include "protobus/errors.h"
#include "protobus/priority.h"

namespace protobus {

MessageListener::MessageListener(std::shared_ptr<Connection> connection, bool lateAck,
                                 std::optional<int> maxConcurrent, RetryOptions retry,
                                 std::optional<int64_t> processingTimeoutMs, std::optional<int> maxPriority)
    : BaseListener(std::move(connection)) {
  // Validated at construction, before any broker I/O, rather than as a 406
  // that closes the channel.
  maxPriority_ = validateMaxPriority(maxPriority);
  if (maxPriority_ && !lateAck) {
    throw InvalidPriorityError(
        "maxPriority requires lateAck. With lateAck off the consumer acks on delivery, RabbitMQ applies no "
        "prefetch, and the broker hands it the entire backlog - leaving priority nothing to reorder. "
        "MessageService enables lateAck by default, so this means an explicit lateAck = false; MessageListener, "
        "constructed directly, defaults it to off and needs it passed. Enable lateAck, or drop maxPriority.");
  }
  exchangeName_ = Config::busExchangeName();
  exchangeType_ = "topic";
  lateAck_ = lateAck;
  maxConcurrent_ = maxConcurrent && *maxConcurrent > 0 ? *maxConcurrent : 1;
  processingTimeoutMs_ = processingTimeoutMs;
  retryConfig_.maxRetries = retry.maxRetries.value_or(kDefaultMaxRetries);
  retryConfig_.retryDelayMs = retry.retryDelayMs.value_or(kDefaultRetryDelayMs);
  retryConfig_.messageTtlMs = retry.messageTtlMs;
  messageTtlMs_ = retry.messageTtlMs;
}

void MessageListener::setupRetryQueues() {
  // No retry for maxRetries 0, or for an anonymous queue, which disappears
  // with the connection and leaves a retry nowhere to come back to.
  if (retryConfig_.maxRetries <= 0 || isAnonymous()) return;
  const std::string service = configuredQueueName();
  auto ch = channel();

  // Messages that have exhausted their retries.
  dlqName_ = service + ".DLQ";
  QueueOptions durable;
  connection_->declareQueue(ch, dlqName_, durable);

  // Messages park here, then expire back to the bus exchange. No
  // x-dead-letter-routing-key: the message keeps its own, which is the
  // original REQUEST.<service>.<method> because it is published to the retry
  // exchange below under that key.
  retryQueueName_ = service + ".Retry";
  QueueOptions retry;
  retry.arguments["x-message-ttl"] = amqp::FieldValue::fromInt(retryConfig_.retryDelayMs);
  retry.arguments["x-dead-letter-exchange"] = amqp::FieldValue::fromString(exchangeName_);
  try {
    connection_->declareQueue(ch, retryQueueName_, retry);
  } catch (const amqp::AmqpError& e) {
    // retryDelayMs is the queue's x-message-ttl, which RabbitMQ fixes at
    // declare time. Say what actually has to happen.
    if (e.preconditionFailed()) {
      throw RetryQueueMismatchError("retry queue '" + retryQueueName_ +
                                    "' already exists with different arguments (most likely a different "
                                    "retryDelayMs - now " +
                                    std::to_string(retryConfig_.retryDelayMs) +
                                    "ms). RabbitMQ cannot change a queue's x-message-ttl in place: drain and "
                                    "delete the queue, or keep the original retryDelayMs. Original error: " +
                                    e.what());
    }
    throw;
  }

  // The retry queue is bound here with `#`, so any routing key lands in it
  // and is preserved on the message.
  retryExchangeName_ = service + ".Retry.Exchange";
  connection_->declareExchange(ch, retryExchangeName_, "topic");
  connection_->bindQueue(ch, retryQueueName_, retryExchangeName_, "#");
}

void MessageListener::restoreTopology() {
  if (!retryQueueName_.empty()) setupRetryQueues();
}

std::optional<ConsumeRetryOptions> MessageListener::getRetryOptions() const {
  if (retryConfig_.maxRetries <= 0 || retryQueueName_.empty() || dlqName_.empty()) return std::nullopt;
  ConsumeRetryOptions o;
  o.maxRetries = retryConfig_.maxRetries;
  o.retryQueueName = retryQueueName_;
  o.retryExchangeName = retryExchangeName_;
  o.dlqName = dlqName_;
  o.isHandledError = [](const std::exception& e) { return isHandledError(e); };
  return o;
}

void MessageListener::subscribe(const std::vector<std::string>& topics) {
  auto ch = channel();
  const std::string queue = queueName();
  for (const auto& topic : topics) {
    connection_->bindQueue(ch, queue, exchangeName_, topic);
    trackBinding(topic);
  }
  setupRetryQueues();
}

}  // namespace protobus
