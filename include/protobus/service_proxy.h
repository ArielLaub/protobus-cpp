// ServiceProxy: calls a service on the bus by name.
//
// Generated `<Service>Proxy` classes wrap one with a typed method per rpc.
// Used directly, it calls methods by name with dynamic messages:
//
//   protobus::ServiceProxy calc(ctx, "Calculator.Service");
//   calc.init();
//   auto req = ctx.factory().newMessage("Calculator.AddRequest");
//   ...
//   auto res = calc.call("add", *req);
//
// The proxy may be constructed with an instance name ("Combat.Player.player6")
// for a service registered under one: requests are routed to
// REQUEST.<instance name>.<method>, and the envelope names the contract
// method, which is what the service validates against.
#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include <google/protobuf/message.h>

#include "protobus/call_options.h"
#include "protobus/context.h"
#include "protobus/custom_types.h"
#include "protobus/errors.h"
#include "protobus/stream.h"

namespace protobus {

class ServiceProxy {
 public:
  ServiceProxy(Context& context, std::string serviceName);

  // Resolve the contract and the methods it declares. Throws
  // InvalidServiceNameError when no loaded service matches the name or a
  // prefix of it, and AlreadyInitializedError on a second call.
  void init();
  bool isInitialized() const { return initialized_; }

  const std::string& serviceName() const { return serviceName_; }
  const std::string& contractServiceName() const { return contract_; }
  std::vector<std::string> methods() const;
  bool isStreaming(const std::string& method) const;

  // Call a unary method. The reply decodes into a dynamic message of the
  // method's response type; with options.rpc false, an empty one returns once
  // the request is confirmed. A service error throws RemoteError; a delivery
  // failure throws its PublishError unchanged.
  std::unique_ptr<google::protobuf::Message> call(const std::string& method,
                                                  const google::protobuf::Message& request,
                                                  const CallOptions& options = {});
  // Call a server-streaming method.
  Stream<std::unique_ptr<google::protobuf::Message>> callStream(const std::string& method,
                                                                const google::protobuf::Message& request,
                                                                const StreamOptions& options = {});

  // Typed forms, used by generated proxies.
  template <typename Res>
  Res callTyped(const std::string& method, const google::protobuf::Message& request, const CallOptions& options) {
    const std::string reply = invoke(method, request, options, false);
    Res result;
    if (!options.rpc) return result;
    decodeInto(method, reply, result);
    return result;
  }

  template <typename Res>
  Stream<Res> callStreamTyped(const std::string& method, const google::protobuf::Message& request,
                              const StreamOptions& options) {
    ChunkStream chunks;
    try {
      chunks = openStream(method, request, options);
    } catch (const InvalidRequestError&) {
      // Surfaced from the first next(), where a caller's try around the loop
      // sees it.
      return Stream<Res>::failed(std::current_exception());
    }
    const std::string full = contract_ + "." + method;
    return Stream<Res>(std::move(chunks), [full](const std::string& chunk) -> std::optional<Res> {
      std::string data;
      if (!decodeChunk(full, chunk, data)) return std::nullopt;
      Res result;
      if (!result.ParseFromString(data)) {
        throw InvalidResponseError("failed parsing streaming chunk for " + full);
      }
      validateCustomTypes(result);
      return result;
    });
  }

 private:
  std::string invoke(const std::string& method, const google::protobuf::Message& request,
                     const CallOptions& options, bool streaming);
  ChunkStream openStream(const std::string& method, const google::protobuf::Message& request,
                         const StreamOptions& options);
  void requireMethod(const std::string& method, bool streaming) const;
  std::string resolveContract() const;
  // The payload of a ResponseContainer, or a thrown RemoteError. False for a
  // container carrying a result with nothing in it to yield.
  static bool decodeChunk(const std::string& method, const std::string& chunk, std::string& data);
  void decodeInto(const std::string& method, const std::string& reply, google::protobuf::Message& out) const;

  Context& context_;
  std::string serviceName_;
  std::string contract_;
  std::set<std::string> methods_;
  std::set<std::string> streaming_;
  bool initialized_ = false;
};

}  // namespace protobus
