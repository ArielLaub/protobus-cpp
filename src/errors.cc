#include "protobus/errors.h"

#include <cxxabi.h>

#include <cstdlib>
#include <memory>
#include <typeinfo>

#include "protobus/config.h"

namespace protobus {

Error::Error(const std::string& message, std::string name, std::string code)
    : std::runtime_error(message), name_(std::move(name)), code_(std::move(code)) {}

HandledError::HandledError(const std::string& message, std::string code)
    : HandledError(message, std::move(code), "HandledError") {}

HandledError::HandledError(const std::string& message, std::string code, std::string name)
    : Error(message, std::move(name), code.empty() ? codes::kHandled : std::move(code)) {}

ProtocolError::ProtocolError(const std::string& message) : ProtocolError(message, "ProtocolError") {}

ProtocolError::ProtocolError(const std::string& message, std::string name)
    : HandledError(message, codes::kProtocol, std::move(name)) {}

InvalidMethodError::InvalidMethodError(const std::string& message) : ProtocolError(message, "InvalidMethodError") {}

InternalServiceError::InternalServiceError(const std::string& correlationId)
    : Error(correlationId.empty() ? "internal service error"
                                  : "internal service error (correlationId " + correlationId + ")",
            "InternalServiceError", codes::kInternal) {}

RemoteError::RemoteError(std::string method, const std::string& message, std::string code)
    : Error(message, "RemoteError", std::move(code)), method_(std::move(method)) {}

RpcTimeoutError::RpcTimeoutError(const std::string& message)
    : Error(message, "RpcTimeoutError", codes::kRpcTimeout) {}

TimeoutError::TimeoutError(const std::string& message)
    : Error(message, "TimeoutError", codes::kProcessingTimeout) {}

PublishError::PublishError(const std::string& message, std::string messageId, std::string name, std::string code)
    : Error(message, std::move(name), std::move(code)), messageId_(std::move(messageId)) {}

PublishNackedError::PublishNackedError(const std::string& message, std::string messageId)
    : PublishError(message, std::move(messageId), "PublishNackedError", codes::kPublishNacked) {}

UnroutableError::UnroutableError(const std::string& message, std::string messageId)
    : PublishError(message, std::move(messageId), "UnroutableError", codes::kUnroutable) {}

PublishConfirmTimeoutError::PublishConfirmTimeoutError(const std::string& message, std::string messageId)
    : PublishError(message, std::move(messageId), "PublishConfirmTimeoutError", codes::kPublishConfirmTimeout) {}

ChannelClosedError::ChannelClosedError(const std::string& message, std::string messageId)
    : PublishError(message, std::move(messageId), "ChannelClosedError", codes::kChannelClosed) {}

StreamingError::StreamingError(const std::string& message, std::string name) : Error(message, std::move(name)) {}

StreamTimeoutError::StreamTimeoutError(const std::string& message) : StreamingError(message, "StreamTimeoutError") {}

StreamBackpressureError::StreamBackpressureError(const std::string& message)
    : StreamingError(message, "StreamBackpressureError") {}

StreamSequenceError::StreamSequenceError(const std::string& message) : StreamingError(message, "StreamSequenceError") {}

NotReadyError::NotReadyError(const std::string& message) : Error(message, "NotReadyError", codes::kNotReady) {}

ReconnectionError::ReconnectionError(const std::string& message) : Error(message, "ReconnectionError") {}

AlreadyConnectedError::AlreadyConnectedError() : Error("already connected", "AlreadyConnectedError") {}

DisconnectedError::DisconnectedError() : Error("Connection lost during RPC call", "DisconnectedError") {}

NotConnectedError::NotConnectedError(const std::string& message) : Error(message, "NotConnectedError") {}

ConnectionError::ConnectionError(const std::string& message) : Error(message, "ConnectionError") {}

NotInitializedError::NotInitializedError(const std::string& message) : Error(message, "NotInitializedError") {}

AlreadyInitializedError::AlreadyInitializedError(const std::string& message)
    : Error(message, "AlreadyInitializedError") {}

AlreadyStartedError::AlreadyStartedError(const std::string& message) : Error(message, "AlreadyStartedError") {}

MissingExchangeError::MissingExchangeError(const std::string& message) : Error(message, "MissingExchangeError") {}

InvalidPriorityError::InvalidPriorityError(const std::string& message) : Error(message, "InvalidPriorityError") {}

InvalidMessageIdError::InvalidMessageIdError(const std::string& message) : Error(message, "InvalidMessageIdError") {}

RetryQueueMismatchError::RetryQueueMismatchError(const std::string& message)
    : Error(message, "RetryQueueMismatchError") {}

MissingProto::MissingProto(const std::string& message) : Error(message, "MissingProto") {}

InvalidServiceNameError::InvalidServiceNameError(const std::string& message)
    : Error(message, "InvalidServiceNameError") {}

InvalidRequestError::InvalidRequestError(const std::string& message) : Error(message, "InvalidRequestError") {}

InvalidResponseError::InvalidResponseError(const std::string& message) : Error(message, "InvalidResponseError") {}

InvalidResultError::InvalidResultError(const std::string& message) : Error(message, "InvalidResultError") {}

InvalidMessageError::InvalidMessageError(const std::string& message) : Error(message, "InvalidMessageError") {}

MessageTypeRequiredError::MessageTypeRequiredError(const std::string& message)
    : Error(message, "MessageTypeRequiredError") {}

InvalidMethodNameError::InvalidMethodNameError(const std::string& message) : Error(message, "InvalidMethodNameError") {}

UnknownMethodError::UnknownMethodError(const std::string& message) : Error(message, "UnknownMethodError") {}

SchemaError::SchemaError(const std::string& message) : Error(message, "SchemaError") {}

CustomTypeConflictError::CustomTypeConflictError(const std::string& message)
    : Error(message, "CustomTypeConflictError") {}

CustomTypeRangeError::CustomTypeRangeError(const std::string& message) : Error(message, "CustomTypeRangeError") {}

// ---- helpers -----------------------------------------------------------------

bool isHandledError(const std::exception& error) noexcept {
  return dynamic_cast<const HandledError*>(&error) != nullptr;
}

namespace {

std::string demangle(const char* mangled) {
  int status = 0;
  std::unique_ptr<char, void (*)(void*)> out(abi::__cxa_demangle(mangled, nullptr, nullptr, &status), std::free);
  return status == 0 && out ? std::string(out.get()) : std::string(mangled);
}

}  // namespace

std::string errorName(const std::exception& error) {
  if (const auto* own = dynamic_cast<const Error*>(&error)) {
    return own->name();
  }
  return demangle(typeid(error).name());
}

std::string errorCode(const std::exception& error) {
  if (const auto* own = dynamic_cast<const Error*>(&error)) {
    return own->code();
  }
  return "";
}

std::string safeErrorSummary(const std::exception* error) {
  if (error == nullptr) {
    return "UnknownError";
  }
  if (isHandledError(*error)) {
    const auto& handled = static_cast<const Error&>(*error);
    return handled.name() + "[" + handled.code() + "]: " + handled.what();
  }
  const std::string name = errorName(*error);
  const std::string code = errorCode(*error);
  return code.empty() ? name : name + "[" + code + "]";
}

CallerError sanitizeErrorForClient(const std::exception& error, const std::string& correlationId) {
  if (isHandledError(error)) {
    return {error.what(), errorCode(error)};
  }
  if (const auto* timeout = dynamic_cast<const TimeoutError*>(&error)) {
    return {timeout->what(), timeout->code()};
  }
  if (Config::exposeInternalErrors()) {
    return {error.what(), errorCode(error)};
  }
  InternalServiceError substitute(correlationId);
  return {substitute.what(), substitute.code()};
}

}  // namespace protobus
