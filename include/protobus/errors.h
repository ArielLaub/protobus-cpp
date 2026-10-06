// The protobus error model.
//
// Every error protobus raises derives from protobus::Error, which carries the
// two things the other ports expose on theirs: a class `name` (what a
// JavaScript error calls `name`) and a machine-readable `code`. Both travel
// further than the message text does: the code crosses the wire in a
// ResponseError, and the name and code together form the x-last-error header
// on retry and dead-letter copies.
#pragma once

#include <exception>
#include <optional>
#include <stdexcept>
#include <string>

namespace protobus {

// Error codes. The first group travels on the wire in a ResponseError and is
// shared with the other ports; the second names local failures, which can
// still reach a further caller when a service relays them.
namespace codes {
inline constexpr const char* kHandled = "HANDLED_ERROR";
inline constexpr const char* kProtocol = "PROTOCOL_ERROR";
inline constexpr const char* kInternal = "INTERNAL_ERROR";
inline constexpr const char* kProcessingTimeout = "PROCESSING_TIMEOUT";

inline constexpr const char* kRpcTimeout = "RPC_TIMEOUT";
inline constexpr const char* kNotReady = "NOT_READY";
inline constexpr const char* kPublishNacked = "PUBLISH_NACKED";
inline constexpr const char* kUnroutable = "UNROUTABLE";
inline constexpr const char* kPublishConfirmTimeout = "PUBLISH_CONFIRM_TIMEOUT";
inline constexpr const char* kChannelClosed = "CHANNEL_CLOSED";
inline constexpr const char* kPublishBacklog = "PUBLISH_BACKLOG";
}  // namespace codes

// Base of every protobus error.
class Error : public std::runtime_error {
 public:
  explicit Error(const std::string& message, std::string name = "Error", std::string code = "");

  // The error's class name, as the TypeScript port names it.
  const std::string& name() const noexcept { return name_; }
  // A machine-readable code, or empty.
  const std::string& code() const noexcept { return code_; }

 protected:
  std::string name_;
  std::string code_;
};

// An error a service raises deliberately to tell its caller something: a
// validation failure, a business rule. It is answered at once and never
// retried, and its message always reaches the caller.
//
// Anything else a handler throws is treated as an infrastructure failure: the
// request is retried and, once retries run out, dead-lettered.
//
//   class ValidationError : public protobus::HandledError {
//    public:
//     explicit ValidationError(const std::string& m) : HandledError(m, "VALIDATION_ERROR") {}
//   };
class HandledError : public Error {
 public:
  explicit HandledError(const std::string& message, std::string code = codes::kHandled);

 protected:
  HandledError(const std::string& message, std::string code, std::string name);
};

// The message could not be understood: it did not decode, or it named
// something this service does not serve. Handled by definition: a malformed
// message is malformed on every redelivery, so retrying it buys nothing.
class ProtocolError : public HandledError {
 public:
  explicit ProtocolError(const std::string& message);

 protected:
  ProtocolError(const std::string& message, std::string name);
};

// The request named a method this service does not serve.
class InvalidMethodError : public ProtocolError {
 public:
  explicit InvalidMethodError(const std::string& message);
};

// Substituted for an unhandled service error before it crosses back to the
// caller, when Config::exposeInternalErrors() is off.
class InternalServiceError : public Error {
 public:
  explicit InternalServiceError(const std::string& correlationId = "");
};

// A failure reported by the remote service: what its ResponseError carried.
// `code()` is empty when the service sent none.
class RemoteError : public Error {
 public:
  RemoteError(std::string method, const std::string& message, std::string code);

  const std::string& method() const noexcept { return method_; }

 private:
  std::string method_;
};

// A unary call got no reply within its timeout.
class RpcTimeoutError : public Error {
 public:
  explicit RpcTimeoutError(const std::string& message);
};

// A service-side attempt overran its processing timeout. The text is
// framework-generated, so it is safe to show a caller whatever the exposure
// setting.
class TimeoutError : public Error {
 public:
  explicit TimeoutError(const std::string& message);
};

// Publish outcomes. A returned publish means the broker confirmed the
// message; these are the ways that can fail.
class PublishError : public Error {
 public:
  PublishError(const std::string& message, std::string messageId, std::string name = "PublishError",
               std::string code = "");

  // Stable across retries of the same logical message, so a consumer can
  // deduplicate on it.
  const std::string& messageId() const noexcept { return messageId_; }

  // Whether the message may have been stored despite the error, so that
  // republishing could duplicate it.
  virtual bool ambiguous() const noexcept { return false; }

 private:
  std::string messageId_;
};

// The broker refused the message (basic.nack). Definite: republishing is safe.
class PublishNackedError : public PublishError {
 public:
  PublishNackedError(const std::string& message, std::string messageId);
};

// A mandatory publish matched no queue, so the broker returned it.
// Definite: for an RPC request it usually means no service is bound.
class UnroutableError : public PublishError {
 public:
  UnroutableError(const std::string& message, std::string messageId);
};

// No confirm arrived in time. AMBIGUOUS: the broker may have stored it.
class PublishConfirmTimeoutError : public PublishError {
 public:
  PublishConfirmTimeoutError(const std::string& message, std::string messageId);
  bool ambiguous() const noexcept override { return true; }
};

// The channel closed with the publish unconfirmed. AMBIGUOUS.
class ChannelClosedError : public PublishError {
 public:
  ChannelClosedError(const std::string& message, std::string messageId);
  bool ambiguous() const noexcept override { return true; }
};

// The publish was refused before it was sent: the channel's
// outstanding-confirm bound was full and its queue of parked publishes too
// (MAX_PARKED_PUBLISHES), or the publish waited out its confirm timeout
// parked. Definite: nothing reached the broker, so republishing is safe.
class PublishBacklogError : public PublishError {
 public:
  PublishBacklogError(const std::string& message, std::string messageId);
};

// Streaming failures, raised on the caller's side.
class StreamingError : public Error {
 public:
  explicit StreamingError(const std::string& message, std::string name = "StreamingError");
};

// No chunk arrived within the idle timeout.
class StreamTimeoutError : public StreamingError {
 public:
  explicit StreamTimeoutError(const std::string& message);
};

// The caller's buffer exceeded its chunk or byte bound.
class StreamBackpressureError : public StreamingError {
 public:
  explicit StreamBackpressureError(const std::string& message);
};

// A chunk was lost: the sequence numbers have a gap.
class StreamSequenceError : public StreamingError {
 public:
  explicit StreamSequenceError(const std::string& message);
};

// Connection state.
class NotReadyError : public Error {
 public:
  explicit NotReadyError(const std::string& message);
};

class ReconnectionError : public Error {
 public:
  explicit ReconnectionError(const std::string& message);
};

class AlreadyConnectedError : public Error {
 public:
  AlreadyConnectedError();
};

// A call was in flight when the connection was lost or closed. The request
// may or may not have been processed.
class DisconnectedError : public Error {
 public:
  explicit DisconnectedError(const std::string& message = "Connection lost during RPC call");
};

class NotConnectedError : public Error {
 public:
  explicit NotConnectedError(const std::string& message = "not connected");
};

class ConnectionError : public Error {
 public:
  explicit ConnectionError(const std::string& message = "connection is not open");
};

class NotInitializedError : public Error {
 public:
  explicit NotInitializedError(const std::string& message = "not initialized");
};

class AlreadyInitializedError : public Error {
 public:
  explicit AlreadyInitializedError(const std::string& message = "already initialized");
};

class AlreadyStartedError : public Error {
 public:
  explicit AlreadyStartedError(const std::string& message = "already started");
};

class MissingExchangeError : public Error {
 public:
  explicit MissingExchangeError(const std::string& message = "listener has no exchange");
};

// Option validation.
class InvalidPriorityError : public Error {
 public:
  explicit InvalidPriorityError(const std::string& message);
};

class InvalidMessageIdError : public Error {
 public:
  explicit InvalidMessageIdError(const std::string& message);
};

// The retry queue exists with different arguments, in practice a changed
// retryDelayMs. RabbitMQ cannot change a queue's TTL in place.
class RetryQueueMismatchError : public Error {
 public:
  explicit RetryQueueMismatchError(const std::string& message);
};

// Schema and dispatch.
class MissingProto : public Error {
 public:
  explicit MissingProto(const std::string& message);
};

class InvalidServiceNameError : public Error {
 public:
  explicit InvalidServiceNameError(const std::string& message);
};

class InvalidRequestError : public Error {
 public:
  explicit InvalidRequestError(const std::string& message);
};

class InvalidResponseError : public Error {
 public:
  explicit InvalidResponseError(const std::string& message);
};

class InvalidResultError : public Error {
 public:
  explicit InvalidResultError(const std::string& message);
};

class InvalidMessageError : public Error {
 public:
  explicit InvalidMessageError(const std::string& message = "invalid message");
};

class MessageTypeRequiredError : public Error {
 public:
  explicit MessageTypeRequiredError(const std::string& message = "message type required");
};

class InvalidMethodNameError : public Error {
 public:
  explicit InvalidMethodNameError(const std::string& message);
};

class UnknownMethodError : public Error {
 public:
  explicit UnknownMethodError(const std::string& message);
};

class SchemaError : public Error {
 public:
  explicit SchemaError(const std::string& message);
};

// A custom type name was re-registered with a different wire type.
class CustomTypeConflictError : public Error {
 public:
  explicit CustomTypeConflictError(const std::string& message);
};

// A value cannot be represented by a custom type's wire format: a bigint
// wider than 32 bytes, an out-of-range timestamp.
class CustomTypeRangeError : public Error {
 public:
  explicit CustomTypeRangeError(const std::string& message);
};

// ---- helpers -----------------------------------------------------------------

// True for a HandledError or anything derived from one.
bool isHandledError(const std::exception& error) noexcept;

// The closest equivalent of a JavaScript error's `name`: the protobus class
// name for our own errors, the demangled C++ type name otherwise.
std::string errorName(const std::exception& error);

// The code an error carries, or empty.
std::string errorCode(const std::exception& error);

// A non-disclosing description of an error for places its text travels
// further than the process (the x-last-error header): its class name and
// code, never an unhandled error's message, which routinely interpolates the
// data that caused it. A HandledError's message is kept: exposing it was the
// point.
std::string safeErrorSummary(const std::exception* error);

// What a caller is told about a failure.
struct CallerError {
  std::string message;
  std::string code;
};

// Decide what an error looks like to the caller. A HandledError crosses as it
// is, and so does a processing timeout, whose text is framework-generated.
// Anything else crosses only when Config::exposeInternalErrors() is on;
// otherwise it becomes an InternalServiceError naming the correlation id.
CallerError sanitizeErrorForClient(const std::exception& error, const std::string& correlationId = "");

}  // namespace protobus
