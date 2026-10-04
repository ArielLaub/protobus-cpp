// MessageFactory: the schemas a process knows, and the envelopes messages
// travel in.
//
// Two kinds of schema are visible through one factory: the types compiled into
// the program (generated C++ classes, found in protobuf's generated pool) and
// the .proto files loaded at runtime with init() or parse(). A service or
// proxy built from generated code needs nothing loaded; the dynamic API
// (ServiceProxy::call, dynamic MessageService handlers, EventListener) decodes
// into google::protobuf::DynamicMessage instances of the factory's types.
//
// A schema that uses a custom type (`bigint`, `timestamp`, or one registered
// with registerType) without importing its definition gets the import added,
// as the other ports do, so shared schemas need no protobus-specific import.
#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "protobus/custom_types.h"

namespace google::protobuf {
class Descriptor;
class DescriptorPool;
class DynamicMessageFactory;
class FileDescriptorProto;
class Message;
class MethodDescriptor;
class ServiceDescriptor;
class SimpleDescriptorDatabase;
class DescriptorPoolDatabase;
class MergedDescriptorDatabase;
}  // namespace google::protobuf

namespace protobus {

struct RequestEnvelope {
  std::string method;
  std::string actor;
  std::string data;  // the undecoded payload
};

struct DecodedResponse {
  // Set for an error response.
  std::optional<std::string> errorMethod;
  std::optional<std::string> errorMessage;
  std::optional<std::string> errorCode;
  // Set for a result.
  std::string method;
  std::string data;  // the undecoded result message

  bool isError() const { return errorMessage.has_value(); }
};

struct DecodedEvent {
  std::string type;
  std::string topic;
  std::string data;  // the undecoded event message
  std::unique_ptr<google::protobuf::Message> message;
};

class MessageFactory {
 public:
  MessageFactory();
  ~MessageFactory();

  MessageFactory(const MessageFactory&) = delete;
  MessageFactory& operator=(const MessageFactory&) = delete;

  // Split "<package>.<Service>.<method>" from the right: the method is the
  // final segment and the service everything before it. Throws
  // InvalidMethodNameError.
  static std::pair<std::string, std::string> splitMethodName(const std::string& fullName);

  // Load every .proto file under each directory (recursively), replacing
  // anything loaded before. A file is named by its path relative to the
  // directory it was found in, which is what its imports refer to.
  void init(const std::vector<std::string>& protoLocations);
  bool isInitialized() const;

  // Add a schema from its text. Idempotent: text already added, or a schema
  // whose types are all present already (compiled in, or loaded from a proto
  // directory), is skipped. Throws SchemaError for a schema that does not
  // parse or conflicts with what is loaded, and NotInitializedError before
  // init().
  void parse(const std::string& proto, const std::string& moduleName = "");

  // Declare a custom type in this factory (and register it process-wide).
  void registerType(const CustomType& type);

  bool hasType(const std::string& fullName) const;
  bool hasService(const std::string& fullName) const;
  const google::protobuf::ServiceDescriptor* findService(const std::string& fullName) const;
  const google::protobuf::Descriptor* findMessageType(const std::string& fullName) const;
  // Throws InvalidMethodNameError or UnknownMethodError.
  const google::protobuf::MethodDescriptor* getMethodType(const std::string& methodFullName) const;
  std::vector<std::string> getServiceMethodNames(const std::string& serviceFullName) const;
  // True when the method is declared `returns (stream T)`. A method missing
  // from the schema is treated as unary.
  bool isStreamingMethod(const std::string& methodFullName) const;

  // A new, empty dynamic message of a type the factory knows.
  std::unique_ptr<google::protobuf::Message> newMessage(const std::string& typeFullName) const;
  // Decode, then check custom-type values against their wire format.
  std::unique_ptr<google::protobuf::Message> decodeMessage(const std::string& typeFullName,
                                                           const std::string& data) const;
  // Check custom-type values, then encode.
  static std::string encodeMessage(const google::protobuf::Message& message);

  // Requests. `obj` must be of the method's request type.
  std::string buildRequest(const std::string& methodFullName, const google::protobuf::Message& obj,
                           const std::string& actor = "") const;
  static RequestEnvelope decodeRequestEnvelope(const std::string& data);
  std::unique_ptr<google::protobuf::Message> decodeRequestPayload(const std::string& methodFullName,
                                                                  const std::string& payload) const;

  // Responses. An error response carries the method only as a label, so it
  // is built without looking the method up: a failure that is ABOUT an
  // unknown method must still be reportable.
  std::string buildResponse(const std::string& methodFullName, const google::protobuf::Message& result) const;
  static std::string buildResultResponse(const std::string& methodFullName, const std::string& encodedResult);
  static std::string buildErrorResponse(const std::string& methodFullName, const std::string& message,
                                        const std::string& code);
  static DecodedResponse decodeResponse(const std::string& data);

  // Events. `obj` must be of type `type`.
  std::string buildEvent(const std::string& type, const google::protobuf::Message& obj, const std::string& topic) const;
  // Decodes the envelope and the payload, as a dynamic message of the
  // envelope's type.
  DecodedEvent decodeEvent(const std::string& data) const;

  const google::protobuf::DescriptorPool& pool() const;

 private:
  struct State;
  void addFile(google::protobuf::FileDescriptorProto file, const std::string& label);
  void injectCustomTypes(google::protobuf::FileDescriptorProto& file);
  void ensureCustomTypeFile(const CustomType& type);
  std::shared_ptr<State> state() const;

  mutable std::mutex mutex_;
  std::shared_ptr<State> state_;
};

}  // namespace protobus
