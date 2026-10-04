// The seam between protobus and an AMQP 0-9-1 client.
//
// protobus talks to the broker only through the interfaces here. The
// production implementation is a thin layer over rabbitmq-c
// (rabbitmqTransport()); tests substitute the in-memory broker of
// <protobus/testing/memory_broker.h>, which has the same semantics and is what
// lets settlement, retry, reconnection and streaming be tested
// deterministically without RabbitMQ.
//
// Threading: an implementation delivers callbacks (deliveries, confirms,
// closes) on a thread of its own, never on the caller's. Callbacks must not
// block. Channel methods that wait for a broker reply (declare, bind, consume,
// ...) must not be called from inside a callback; publish, ack and reject may
// be, and never block on the broker.
#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "protobus/errors.h"

namespace protobus::amqp {

struct FieldValue;
using FieldTable = std::map<std::string, FieldValue>;
using FieldArray = std::vector<FieldValue>;

// An AMQP field-table value. The kind is kept, so a header read from one
// delivery is written back with the same encoding when the message is
// republished on the retry and dead-letter paths.
struct FieldValue {
  enum class Kind {
    Void,
    Bool,
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
    U64,
    Float,
    Double,
    Decimal,
    String,  // longstr: UTF-8 text or arbitrary bytes
    Bytes,   // byte array ('x')
    Timestamp,
    Table,
    Array,
  };

  Kind kind = Kind::Void;
  bool boolean = false;
  int64_t integer = 0;    // every signed kind, and Timestamp
  uint64_t unsigned_ = 0; // U8/U16/U32/U64
  double real = 0;        // Float/Double
  uint8_t decimalScale = 0;
  uint32_t decimalValue = 0;
  std::string text;  // String/Bytes
  std::shared_ptr<FieldTable> table;
  std::shared_ptr<FieldArray> array;

  static FieldValue fromBool(bool v);
  static FieldValue fromInt(int64_t v);  // written as a signed 64-bit integer
  static FieldValue fromInt32(int32_t v);
  static FieldValue fromString(std::string v);
  static FieldValue fromDouble(double v);
  static FieldValue fromTable(FieldTable v);
  static FieldValue fromArray(FieldArray v);

  // Tolerant readers, accepting every encoding peers produce: integers of
  // any width, or as decimal text; booleans as a boolean, number or text.
  std::optional<int64_t> asInt() const;
  std::optional<bool> asBool() const;
  // Text for String/Bytes, a decimal rendering for numbers, nullopt otherwise.
  std::optional<std::string> asString() const;

  bool operator==(const FieldValue& other) const;
};

// Basic properties. Absent stays absent: nothing is written for an unset
// property.
struct Properties {
  std::optional<std::string> contentType;
  std::optional<std::string> contentEncoding;
  std::optional<FieldTable> headers;
  std::optional<uint8_t> deliveryMode;  // 2 = persistent
  std::optional<uint8_t> priority;
  std::optional<std::string> correlationId;
  std::optional<std::string> replyTo;
  std::optional<std::string> expiration;
  std::optional<std::string> messageId;
  std::optional<uint64_t> timestamp;
  std::optional<std::string> type;
  std::optional<std::string> userId;
  std::optional<std::string> appId;
};

struct Delivery {
  std::string body;
  Properties properties;
  std::string exchange;
  std::string routingKey;
  std::string consumerTag;
  uint64_t deliveryTag = 0;
  bool redelivered = false;
};

// How the broker answered a publish on a confirm channel.
enum class ConfirmOutcome {
  Ack,       // stored
  Nack,      // refused (basic.nack)
  Returned,  // a mandatory publish matched no queue: returned, then acked
  Closed,    // the channel closed with the publish unconfirmed: UNKNOWN
};

// A broker-reported failure: a channel or connection exception
// (`replyCode` 404, 406, ...) or a client-side transport failure (0).
class AmqpError : public Error {
 public:
  AmqpError(const std::string& message, int replyCode = 0, bool channelClosed = true);

  int replyCode() const noexcept { return replyCode_; }
  // Whether the error closed the channel it happened on.
  bool channelClosed() const noexcept { return channelClosed_; }
  // 406 PRECONDITION_FAILED: a declare whose arguments disagree with the
  // existing object's.
  bool preconditionFailed() const noexcept { return replyCode_ == 406; }

 private:
  int replyCode_;
  bool channelClosed_;
};

using DeliveryCallback = std::function<void(Delivery)>;
using ConfirmCallback = std::function<void(ConfirmOutcome, std::string detail)>;

class Channel {
 public:
  virtual ~Channel() = default;

  // Every Channel is a confirm channel: publish() reports the broker's
  // answer through its callback.

  virtual void declareExchange(const std::string& name, const std::string& type, bool durable, bool autoDelete,
                               bool internal, const FieldTable& arguments) = 0;
  // Returns the queue's name: the given one, or the broker's for "".
  virtual std::string declareQueue(const std::string& name, bool durable, bool exclusive, bool autoDelete,
                                   const FieldTable& arguments) = 0;
  virtual void bindQueue(const std::string& queue, const std::string& exchange, const std::string& routingKey,
                         const FieldTable& arguments) = 0;
  virtual void unbindQueue(const std::string& queue, const std::string& exchange, const std::string& routingKey,
                           const FieldTable& arguments) = 0;
  virtual void deleteQueue(const std::string& name) = 0;
  virtual void purgeQueue(const std::string& name) = 0;
  virtual void prefetch(uint16_t count) = 0;

  // Start a consumer. `onDelivery` runs on the transport's thread; `onCancel`
  // when the broker cancels the consumer (its queue was deleted, say).
  virtual std::string consume(const std::string& queue, const std::string& consumerTag, bool noAck, bool exclusive,
                              DeliveryCallback onDelivery, std::function<void()> onCancel) = 0;
  virtual void cancel(const std::string& consumerTag) = 0;

  virtual void ack(uint64_t deliveryTag) = 0;
  virtual void reject(uint64_t deliveryTag, bool requeue) = 0;

  // Publish. `onConfirm` is called exactly once, on the transport's thread.
  // Throws AmqpError only when the publish could not be written at all, in
  // which case `onConfirm` is never called.
  virtual void publish(const std::string& exchange, const std::string& routingKey, const std::string& body,
                       const Properties& properties, bool mandatory, ConfirmCallback onConfirm) = 0;

  virtual void close() = 0;
  virtual bool isOpen() const = 0;
  // Called once when the channel closes for any reason; at once if it has.
  virtual void onClose(std::function<void(const std::string& reason)> fn) = 0;
};

class Connection {
 public:
  virtual ~Connection() = default;

  virtual std::shared_ptr<Channel> openChannel() = 0;
  // A graceful close. The close callback then reports no error.
  virtual void close() = 0;
  virtual bool isOpen() const = 0;
  // Called once when the connection closes: with nullopt after close(), with
  // the reason when it was lost. At once if it already has.
  virtual void onClose(std::function<void(std::optional<std::string> error)> fn) = 0;
};

// Opens connections.
class Transport {
 public:
  virtual ~Transport() = default;

  // Connect and log in. `heartbeatSeconds` applies unless the URL carries a
  // `heartbeat` parameter, which wins (0 disables heartbeats). Throws
  // AmqpError on failure.
  virtual std::shared_ptr<Connection> connect(const std::string& url, int heartbeatSeconds) = 0;
};

// The production transport, over rabbitmq-c. Supports amqp:// and amqps://
// URLs with the RabbitMQ URI query parameters heartbeat, connection_timeout,
// channel_max, frame_max, cacertfile, certfile, keyfile and verify.
std::shared_ptr<Transport> rabbitmqTransport();

}  // namespace protobus::amqp
