#include "protobus/message_service.h"

#include <fstream>
#include <sstream>

#include "protobus/config.h"
#include "protobus/logger.h"

namespace protobus {

namespace {

// The final dot-separated segment: the bare method name in both
// `Combat.Player.shoot` and `REQUEST.Combat.Player.player6.shoot`.
std::string lastSegment(const std::string& value) {
  const auto i = value.rfind('.');
  return i == std::string::npos ? value : value.substr(i + 1);
}

}  // namespace

namespace {

// Wraps each chunk of a stream in a ResponseContainer. A failure during
// iteration becomes the stream's terminal error response, which the
// connection layer publishes with x-protobus-final=true so the caller's
// iteration raises it. `service` keeps the service alive while the stream
// runs.
Generator<std::string> streamResponses(std::string method, Generator<std::string> inner, std::string correlationId,
                                       std::shared_ptr<void> service) {
  std::optional<std::string> failure;
  try {
    while (auto chunk = inner.next()) co_yield MessageFactory::buildResultResponse(method, *chunk);
  } catch (const std::exception& e) {
    Logger::error(e.what());
    // The stream's terminal error is published to the caller, so it gets the
    // sanitised form, as on the unary path.
    const auto c = sanitizeErrorForClient(e, correlationId);
    failure = MessageFactory::buildErrorResponse(method, c.message, c.code);
  } catch (...) {
    InternalServiceError substitute(correlationId);
    failure = MessageFactory::buildErrorResponse(method, substitute.what(), substitute.code());
  }
  if (failure) co_yield *failure;
}

}  // namespace

MessageService::MessageService(Context& context, MessageServiceOptions options)
    : context_(context), options_(std::move(options)) {
  listener_ = std::make_shared<MessageListener>(context_.connectionPtr(), options_.lateAck.value_or(true),
                                                options_.maxConcurrent, options_.retry,
                                                options_.processingTimeoutMs, options_.maxPriority);
  eventListener_ =
      std::make_shared<EventListener>(context_.connectionPtr(), context_.factoryPtr(), options_.eventRetry);
  cancelListener_ = std::make_shared<CancelListener>(context_.connectionPtr());
}

MessageService::~MessageService() {
  try {
    close();
  } catch (...) {
  }
}

std::string MessageService::ProtoFileName() const { return ""; }

std::string MessageService::Proto() const {
  const std::string file = ProtoFileName();
  std::ifstream in(file, std::ios::binary);
  if (file.empty() || !in) throw MissingProto("missing_proto_source");
  std::stringstream buf;
  buf << in.rdbuf();
  return buf.str();
}

std::string MessageService::contractServiceName() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return contractServiceName_.value_or("");
}

void MessageService::addMethod(const std::string& name, MethodEntry entry) {
  std::lock_guard<std::mutex> lock(mutex_);
  methods_[name] = std::move(entry);
}

void MessageService::registerMethod(const std::string& name, UnaryMethod handler) {
  MethodEntry entry;
  entry.streaming = false;
  Context* ctx = &context_;
  entry.unary = [handler, ctx](const std::string& payload, CallContext& call) {
    std::unique_ptr<google::protobuf::Message> request;
    try {
      request = ctx->factory().decodeRequestPayload(call.method, payload);
    } catch (const std::exception&) {
      throw ProtocolError("payload did not decode as the request type of " + call.method);
    }
    auto result = handler(*request, call);
    if (!result) throw InvalidResultError("method " + call.method + " returned no result");
    const auto* method = ctx->factory().getMethodType(call.method);
    if (result->GetDescriptor()->full_name() != method->output_type()->full_name()) {
      throw InvalidResultError("result of " + call.method + " must be a " +
                               std::string(method->output_type()->full_name()) + ", got a " +
                               std::string(result->GetDescriptor()->full_name()));
    }
    return MessageFactory::encodeMessage(*result);
  };
  addMethod(name, std::move(entry));
}

namespace {
Generator<std::string> runDynamicStream(std::unique_ptr<google::protobuf::Message> request,
                                        std::shared_ptr<CallContext> ctx,
                                        std::function<Generator<std::unique_ptr<google::protobuf::Message>>(
                                            const google::protobuf::Message&, CallContext&)>
                                            handler) {
  auto chunks = handler(*request, *ctx);
  while (auto chunk = chunks.next()) co_yield MessageFactory::encodeMessage(**chunk);
}
}  // namespace

void MessageService::registerStreamingMethod(const std::string& name, StreamMethod handler) {
  MethodEntry entry;
  entry.streaming = true;
  Context* ctx = &context_;
  entry.stream = [handler, ctx](const std::string& payload, std::shared_ptr<CallContext> call) {
    std::unique_ptr<google::protobuf::Message> request;
    try {
      request = ctx->factory().decodeRequestPayload(call->method, payload);
    } catch (const std::exception&) {
      throw ProtocolError("payload did not decode as the request type of " + call->method);
    }
    return runDynamicStream(std::move(request), std::move(call), handler);
  };
  addMethod(name, std::move(entry));
}

void MessageService::parseRequest(google::protobuf::Message& request, const std::string& payload,
                                  const std::string& method) {
  if (!request.ParseFromString(payload)) {
    // Type name and size only: a payload that failed to decode is still a
    // payload.
    Logger::error("unparseable request payload for " + method + " (" + std::to_string(payload.size()) + " bytes)");
    throw ProtocolError("payload did not decode as the request type of " + method);
  }
  try {
    validateCustomTypes(request);
  } catch (const std::exception& e) {
    throw ProtocolError("payload of " + method + " carries an invalid custom-type value: " + e.what());
  }
}

// Make sure the service's schema is loaded. Skipped when the contract
// already resolves: compiled in, or loaded from a proto directory.
void MessageService::registerSchema() {
  if (context_.factory().hasService(ServiceName())) return;
  if (tryResolveContract()) return;
  context_.factory().parse(Proto(), ServiceName());
}

bool MessageService::tryResolveContract() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (contractServiceName_) return true;
  }
  auto& factory = context_.factory();
  std::string candidate = ServiceName();
  for (;;) {
    if (factory.hasService(candidate)) {
      auto names = factory.getServiceMethodNames(candidate);
      std::lock_guard<std::mutex> lock(mutex_);
      contractServiceName_ = candidate;
      declaredMethods_ = std::set<std::string>(names.begin(), names.end());
      return true;
    }
    const auto cut = candidate.rfind('.');
    if (cut == std::string::npos || cut == 0) return false;
    candidate = candidate.substr(0, cut);
  }
}

// Find the contract by trimming runtime segments off ServiceName until one
// names a loaded service.
void MessageService::resolveContract() {
  if (tryResolveContract()) return;
  throw MissingProto("no service in the schema matches '" + ServiceName() +
                     "' or any prefix of it; the .proto must declare the service this class serves");
}

void MessageService::init() {
  try {
    registerSchema();
    resolveContract();
    // Every delivery holds the service for as long as its handler runs.
    std::weak_ptr<MessageService> weak = weak_from_this();
    if (weak.expired()) {
      throw std::logic_error("service " + ServiceName() +
                             " must be owned by a std::shared_ptr: construct it with std::make_shared (or "
                             "RunnableService::start) before calling init()");
    }
    listener_->setErrorReplyBuilder(
        [weak](const std::string& content, const std::exception& error) -> std::optional<std::string> {
          auto self = weak.lock();
          if (!self) return std::nullopt;
          return self->buildTimeoutReply(content, error);
        });
    listener_->init(
        [weak](const std::string& data, const std::string& correlationId,
               MessageHandlerContext& context) -> MessageHandlerResult {
          auto self = weak.lock();
          if (!self) throw NotInitializedError("the service has been destroyed");
          // Kept until the handler's abort listeners are done too. A stream
          // runs after this returns, so onMessage hands it a reference of its
          // own.
          context.keepAlive = self;
          return self->onMessage(data, correlationId, context);
        },
        ServiceName());
    eventListener_->init(nullptr, ServiceName() + ".Events");
    listener_->subscribe("REQUEST." + ServiceName() + ".*");
    listener_->start();
    eventListener_->start();
    // Started last: it only matters once requests can arrive.
    cancelListener_->start();
    std::lock_guard<std::mutex> lock(mutex_);
    initialized_ = true;
  } catch (const std::exception& e) {
    Logger::error("error initializing service " + ServiceName() + " - " + e.what());
    // Nothing of a service that failed to start keeps running.
    try {
      stopConsuming();
    } catch (...) {
    }
    for (BaseListener* l : {static_cast<BaseListener*>(listener_.get()), static_cast<BaseListener*>(eventListener_.get())}) {
      try {
        if (l->isInitialized()) l->close();
      } catch (...) {
      }
    }
    throw;
  }
}

void MessageService::stopConsuming() {
  listener_->stopConsuming();
  eventListener_->stopConsuming();
  // Closed with the rest: a drained service has no stream left to cancel.
  cancelListener_->close();
}

void MessageService::close() {
  bool wasInitialized;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    wasInitialized = initialized_;
    initialized_ = false;
  }
  if (!wasInitialized) return;
  stopConsuming();
  if (listener_->isInitialized()) listener_->close();
  if (eventListener_->isInitialized()) eventListener_->close();
}

void MessageService::publishEvent(const std::string& type, const google::protobuf::Message& content,
                                  const std::string& topic) {
  context_.publishEvent(type, content, topic);
}

void MessageService::publishEvent(const google::protobuf::Message& content, const std::string& topic) {
  context_.publishEvent(content, topic);
}

void MessageService::subscribeEvent(const std::string& type, EventHandler handler, const std::string& topic) {
  eventListener_->subscribe(type, std::move(handler), topic);
}

std::string MessageService::protocolError(const std::string& label, const std::string& reason) const {
  ProtocolError error(reason);
  return MessageFactory::buildErrorResponse(label.empty() ? "unknown" : label, error.what(), error.code());
}

// A HandledError is expected (validation, business rules): it is answered
// as a normal error response, at once, and never retried. Anything else is an
// infrastructure failure, rethrown so the connection layer's retry ladder
// takes over, carrying the sanitised reply the caller receives once retries
// are spent.
std::string MessageService::handleUnaryError(const std::string& method, std::exception_ptr error,
                                             const std::string& correlationId) {
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& e) {
    if (isHandledError(e)) {
      Logger::warn("handled error in " + method + ": " + e.what());
      return MessageFactory::buildErrorResponse(method, e.what(), errorCode(e));
    }
    Logger::error(e.what());
    // What the CALLER sees is sanitised; what is logged above is the real
    // error.
    const auto c = sanitizeErrorForClient(e, correlationId);
    throw ErrorWithReply(error, MessageFactory::buildErrorResponse(method, c.message, c.code));
  } catch (...) {
    Logger::error("a handler threw a non-standard exception");
    InternalServiceError substitute(correlationId);
    throw ErrorWithReply(error, MessageFactory::buildErrorResponse(method, substitute.what(), substitute.code()));
  }
}

// The reply for a failure that reached the connection layer without one: a
// processing timeout. The method label comes from the envelope, read again.
std::optional<std::string> MessageService::buildTimeoutReply(const std::string& content,
                                                             const std::exception& error) const {
  std::string method = contractServiceName_.value_or(ServiceName());
  try {
    method = MessageFactory::decodeRequestEnvelope(content).method;
  } catch (...) {
  }
  const auto c = sanitizeErrorForClient(error);
  return MessageFactory::buildErrorResponse(method, c.message, c.code);
}

MessageHandlerResult MessageService::onMessage(const std::string& data, const std::string& correlationId,
                                               MessageHandlerContext& hctx) {
  resolveContract();
  std::string contract;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    contract = *contractServiceName_;
  }
  const std::string serviceName = ServiceName();

  // Envelope first, payload later. The envelope names the method, and that
  // name selects the schema the payload is read with, so it is checked
  // against this service's contract before the bytes are interpreted.
  RequestEnvelope envelope;
  try {
    envelope = MessageFactory::decodeRequestEnvelope(data);
  } catch (const std::exception&) {
    Logger::error("unparseable request envelope on " + serviceName + " (" + std::to_string(data.size()) +
                  " bytes, " + correlationId + ")");
    return protocolError(hctx.routingKey, "request envelope did not decode");
  }
  Logger::debug("received request " + envelope.method + " (" + correlationId + ")");

  // The method to run comes from the message body, so it is checked against
  // what the broker actually routed and against this service's own name.
  // Without this, a client that can publish to the bus picks the method
  // regardless of the routing key, which makes RabbitMQ topic permissions
  // unenforceable. A rejection is reported against the method the ROUTING
  // KEY names: the body's name is exactly what is in dispute.
  const std::string& routingKey = hctx.routingKey;
  const std::string contractMethod =
      contract + "." + (!routingKey.empty() ? lastSegment(routingKey) : lastSegment(envelope.method));
  auto rejectDispatch = [&](const std::string& reason) {
    InvalidMethodError error(reason);
    Logger::error(reason);
    return MessageFactory::buildErrorResponse(contractMethod, error.what(), error.code());
  };

  // 1. The delivery belongs to THIS service, judged by the routing key the
  //    broker used, not by the publisher-controlled body.
  // 2. The body asks for the method the routing key names.
  if (!routingKey.empty()) {
    if (routingKey.rfind("REQUEST." + serviceName + ".", 0) != 0) {
      return rejectDispatch("routing key " + routingKey + " does not belong to service " + serviceName);
    }
    if (lastSegment(routingKey) != lastSegment(envelope.method)) {
      return rejectDispatch("request method " + envelope.method + " contradicts routing key " + routingKey);
    }
  }

  // 3. The body names a method of THIS contract, spelled in full, so a name
  //    cannot carry extra segments, nor name another loaded service.
  std::string method;
  try {
    auto [svc, m] = MessageFactory::splitMethodName(envelope.method);
    if (svc != contract) {
      return rejectDispatch("request method " + envelope.method + " is not a method of " + contract);
    }
    method = m;
  } catch (const InvalidMethodNameError&) {
    return rejectDispatch("request method " + envelope.method + " is not a qualified method name");
  }

  MethodEntry entry;
  bool declared;
  bool found;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    declared = declaredMethods_.count(method) > 0;
    auto it = methods_.find(method);
    found = declared && it != methods_.end();
    if (found) entry = it->second;
  }
  if (!declared) return rejectDispatch(contract + " declares no method " + method);
  // Only what this service implements is dispatchable.
  if (!found) {
    InvalidMethodError error("invalid service method " + method);
    Logger::error(error.what());
    return MessageFactory::buildErrorResponse(envelope.method, error.what(), error.code());
  }

  auto call = std::make_shared<CallContext>();
  call->actor = envelope.actor;
  call->correlationId = correlationId;
  call->method = envelope.method;
  call->signal = hctx.signal;
  call->routingKey = hctx.routingKey;
  call->messageId = hctx.messageId;
  call->redelivered = hctx.redelivered;
  call->headers = hctx.headers;

  if (context_.factory().isStreamingMethod(envelope.method)) {
    if (!entry.streaming) {
      InvalidResultError error("streaming method " + method + " must be implemented as a stream");
      return MessageFactory::buildErrorResponse(envelope.method, error.what(), error.code());
    }
    Generator<std::string> chunks;
    try {
      chunks = entry.stream(envelope.data, call);
    } catch (...) {
      // A throw before the stream exists (a malformed request, an eager
      // HandledError) is answered as the unary path answers one.
      return handleUnaryError(envelope.method, std::current_exception(), correlationId);
    }
    return streamResponses(envelope.method, std::move(chunks), correlationId, shared_from_this());
  }

  if (entry.streaming) {
    InvalidResultError error("unary method " + method + " must not be implemented as a stream");
    return MessageFactory::buildErrorResponse(envelope.method, error.what(), error.code());
  }
  try {
    std::string result = entry.unary(envelope.data, *call);
    // No payload in the log line: responses carry secrets and PII.
    Logger::debug("sending result " + envelope.method + " (" + correlationId + ")");
    return MessageFactory::buildResultResponse(envelope.method, result);
  } catch (...) {
    return handleUnaryError(envelope.method, std::current_exception(), correlationId);
  }
}

}  // namespace protobus
