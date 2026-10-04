#include "protobus/service_proxy.h"

#include <google/protobuf/descriptor.h>

#include "protobus/logger.h"
#include "protobus/message_factory.h"

namespace protobus {

ServiceProxy::ServiceProxy(Context& context, std::string serviceName)
    : context_(context), serviceName_(std::move(serviceName)) {}

// Trims runtime segments off the name until one names a loaded service, as
// MessageService resolves its own, so the two agree by construction.
std::string ServiceProxy::resolveContract() const {
  auto& factory = context_.factory();
  if (!factory.isInitialized()) {
    throw InvalidServiceNameError("cannot resolve '" + serviceName_ +
                                  "': the message factory has not been initialised, so no schema is loaded yet. "
                                  "Call Context::init() before initialising a proxy.");
  }
  std::string candidate = serviceName_;
  for (;;) {
    if (factory.hasService(candidate)) {
      if (candidate != serviceName_) {
        // Said out loud, because trimming is a guess: a name whose intended
        // service is absent resolves to an unrelated ancestor, and every
        // call then fails as unroutable.
        Logger::info("service proxy '" + serviceName_ + "' resolved to contract '" + candidate +
                     "'; requests will route to REQUEST." + serviceName_ + ".*");
      }
      return candidate;
    }
    const auto cut = candidate.rfind('.');
    if (cut == std::string::npos || cut == 0) {
      throw InvalidServiceNameError("no service in the schema matches '" + serviceName_ +
                                    "' or any prefix of it; the .proto must declare the service this proxy "
                                    "addresses");
    }
    candidate = candidate.substr(0, cut);
  }
}

void ServiceProxy::init() {
  if (initialized_) {
    Logger::error("already initialized service proxy " + serviceName_);
    throw AlreadyInitializedError("service proxy " + serviceName_ + " is already initialized");
  }
  contract_ = resolveContract();
  const auto* service = context_.factory().findService(contract_);
  if (service == nullptr) throw InvalidServiceNameError("no such service " + contract_);
  for (int i = 0; i < service->method_count(); ++i) {
    const auto* m = service->method(i);
    methods_.insert(std::string(m->name()));
    if (m->server_streaming()) streaming_.insert(std::string(m->name()));
  }
  initialized_ = true;
}

std::vector<std::string> ServiceProxy::methods() const { return {methods_.begin(), methods_.end()}; }

bool ServiceProxy::isStreaming(const std::string& method) const { return streaming_.count(method) > 0; }

void ServiceProxy::requireMethod(const std::string& method, bool streaming) const {
  if (!initialized_) throw NotInitializedError("service proxy " + serviceName_ + " is not initialized");
  if (!methods_.count(method)) {
    throw UnknownMethodError("service '" + contract_ + "' declares no method '" + method + "'");
  }
  if (isStreaming(method) != streaming) {
    throw InvalidRequestError(contract_ + "." + method + (streaming ? " is unary: use call()" :
                                                                      " is server-streaming: use callStream()"));
  }
}

std::string ServiceProxy::invoke(const std::string& method, const google::protobuf::Message& request,
                                 const CallOptions& options, bool streaming) {
  requireMethod(method, streaming);
  // The ENVELOPE carries the contract method name: it is what the receiving
  // service validates the body against. The ROUTING KEY carries the runtime
  // name: it is what reaches this instance's queue.
  const std::string full = contract_ + "." + method;
  const std::string routingKey = "REQUEST." + serviceName_ + "." + method;
  std::string buffer;
  try {
    buffer = context_.factory().buildRequest(full, request, options.actor);
  } catch (const std::exception& e) {
    // No payload in the log line: requests carry secrets and PII.
    Logger::error("failed building message for " + full + ": " + e.what());
    throw InvalidRequestError(std::string("failed parsing message: ") + e.what());
  }
  // The delivery error is raised as it stands: UnroutableError and
  // PublishNackedError are definite and safe to retry, while
  // PublishConfirmTimeoutError and ChannelClosedError are ambiguous.
  return context_.publishMessage(buffer, routingKey, options);
}

ChunkStream ServiceProxy::openStream(const std::string& method, const google::protobuf::Message& request,
                                     const StreamOptions& options) {
  requireMethod(method, true);
  const std::string full = contract_ + "." + method;
  const std::string routingKey = "REQUEST." + serviceName_ + "." + method;
  std::string buffer;
  try {
    buffer = context_.factory().buildRequest(full, request, options.actor);
  } catch (const std::exception& e) {
    Logger::error("failed building streaming request for " + full + ": " + e.what());
    throw InvalidRequestError(std::string("failed parsing message: ") + e.what());
  }
  return context_.publishStreamingMessage(buffer, routingKey, options);
}

bool ServiceProxy::decodeChunk(const std::string& method, const std::string& chunk, std::string& data) {
  DecodedResponse response;
  try {
    response = MessageFactory::decodeResponse(chunk);
  } catch (const std::exception& e) {
    Logger::error(e.what());
    throw InvalidResponseError("failed parsing result for " + method);
  }
  // A terminal chunk may carry an error instead of a result.
  if (response.isError()) {
    throw RemoteError(response.errorMethod.value_or(method), *response.errorMessage,
                      response.errorCode.value_or(""));
  }
  data = std::move(response.data);
  return true;
}

void ServiceProxy::decodeInto(const std::string& method, const std::string& reply,
                              google::protobuf::Message& out) const {
  const std::string full = contract_ + "." + method;
  std::string data;
  decodeChunk(full, reply, data);
  if (!out.ParseFromString(data)) throw InvalidResponseError("failed parsing result for " + full);
  validateCustomTypes(out);
}

std::unique_ptr<google::protobuf::Message> ServiceProxy::call(const std::string& method,
                                                              const google::protobuf::Message& request,
                                                              const CallOptions& options) {
  const std::string reply = invoke(method, request, options, false);
  const std::string full = contract_ + "." + method;
  const std::string responseType(context_.factory().getMethodType(full)->output_type()->full_name());
  auto result = context_.factory().newMessage(responseType);
  if (!options.rpc) {
    Logger::debug("received non rpc result, sending back an empty answer");
    return result;
  }
  decodeInto(method, reply, *result);
  return result;
}

Stream<std::unique_ptr<google::protobuf::Message>> ServiceProxy::callStream(const std::string& method,
                                                                            const google::protobuf::Message& request,
                                                                            const StreamOptions& options) {
  using Ptr = std::unique_ptr<google::protobuf::Message>;
  ChunkStream chunks;
  try {
    chunks = openStream(method, request, options);
  } catch (const InvalidRequestError&) {
    return Stream<Ptr>::failed(std::current_exception());
  }
  const std::string full = contract_ + "." + method;
  const std::string responseType(context_.factory().getMethodType(full)->output_type()->full_name());
  Context* ctx = &context_;
  return Stream<Ptr>(std::move(chunks), [full, responseType, ctx](const std::string& chunk) -> std::optional<Ptr> {
    std::string data;
    if (!decodeChunk(full, chunk, data)) return std::nullopt;
    try {
      return ctx->factory().decodeMessage(responseType, data);
    } catch (const std::exception&) {
      throw InvalidResponseError("failed parsing streaming chunk for " + full);
    }
  });
}

}  // namespace protobus
