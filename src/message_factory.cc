#include "protobus/message_factory.h"

#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/descriptor_database.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <functional>
#include <sstream>

#include "protobus/errors.h"
#include "protobus/logger.h"
#include "protobus/types.pb.h"
#include "wire/envelope.h"

namespace protobus {

namespace gpb = google::protobuf;

namespace {

constexpr const char* kTypesFile = "protobus/types.proto";

std::string customTypeFile(const std::string& name) { return "protobus/custom/" + name + ".proto"; }

class CollectingErrors : public gpb::io::ErrorCollector {
 public:
  explicit CollectingErrors(std::string label) : label_(std::move(label)) {}
  void RecordError(int line, gpb::io::ColumnNumber column, absl::string_view message) override {
    if (!text_.empty()) text_ += "; ";
    text_ += "(" + label_ + ", line " + std::to_string(line + 1) + ":" + std::to_string(column + 1) + ") " +
             std::string(message);
  }
  const std::string& text() const { return text_; }

 private:
  std::string label_;
  std::string text_;
};

class PoolErrors : public gpb::DescriptorPool::ErrorCollector {
 public:
  void RecordError(absl::string_view filename, absl::string_view element, const gpb::Message*,
                   ErrorLocation, absl::string_view message) override {
    if (!text_.empty()) text_ += "; ";
    text_ += std::string(filename) + ": " + std::string(element) + ": " + std::string(message);
  }
  std::string take() {
    std::string t = std::move(text_);
    text_.clear();
    return t;
  }

 private:
  std::string text_;
};

// Every symbol a file declares at its top level, fully qualified.
std::vector<std::string> topLevelSymbols(const gpb::FileDescriptorProto& file) {
  const std::string prefix = file.package().empty() ? "" : file.package() + ".";
  std::vector<std::string> out;
  for (const auto& m : file.message_type()) out.push_back(prefix + m.name());
  for (const auto& e : file.enum_type()) out.push_back(prefix + e.name());
  for (const auto& s : file.service()) out.push_back(prefix + s.name());
  return out;
}

// Type names a file refers to, as written (unresolved).
void referencedTypes(const gpb::DescriptorProto& m, std::set<std::string>& out, std::set<std::string>& declared,
                     const std::string& scope) {
  declared.insert(scope + m.name());
  for (const auto& f : m.field()) {
    if (!f.type_name().empty()) out.insert(f.type_name());
  }
  for (const auto& nested : m.nested_type()) referencedTypes(nested, out, declared, scope + m.name() + ".");
}

}  // namespace

struct MessageFactory::State {
  gpb::SimpleDescriptorDatabase runtime;
  gpb::DescriptorPoolDatabase generated{*gpb::DescriptorPool::generated_pool()};
  gpb::MergedDescriptorDatabase merged{&runtime, &generated};
  PoolErrors errors;
  std::unique_ptr<gpb::DescriptorPool> pool;
  std::unique_ptr<gpb::DynamicMessageFactory> dynamic;
  std::set<std::string> parsedSchemas;
  std::set<std::string> runtimeFiles;
  std::mutex buildMutex;

  State() {
    pool = std::make_unique<gpb::DescriptorPool>(&merged, &errors);
    dynamic = std::make_unique<gpb::DynamicMessageFactory>(pool.get());
  }
};

MessageFactory::MessageFactory() {
  // Make sure the built-in custom types are linked and in the generated pool.
  (void)::bigint::descriptor();
  (void)::timestamp::descriptor();
}

MessageFactory::~MessageFactory() = default;

std::shared_ptr<MessageFactory::State> MessageFactory::state() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return state_;
}

bool MessageFactory::isInitialized() const { return state() != nullptr; }

const gpb::DescriptorPool& MessageFactory::pool() const {
  auto st = state();
  if (!st) throw NotInitializedError("message factory not initialized");
  return *st->pool;
}

std::pair<std::string, std::string> MessageFactory::splitMethodName(const std::string& fullName) {
  const auto i = fullName.rfind('.');
  if (i == std::string::npos || i == 0 || i == fullName.size() - 1) {
    throw InvalidMethodNameError("'" + fullName +
                                 "' is not a fully-qualified method name (<package>.<Service>.<method>)");
  }
  return {fullName.substr(0, i), fullName.substr(i + 1)};
}

void MessageFactory::init(const std::vector<std::string>& protoLocations) {
  namespace fs = std::filesystem;
  // A fresh state: what an earlier init() loaded is forgotten.
  auto fresh = std::make_shared<State>();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    state_ = fresh;
  }
  for (const auto& t : getCustomTypes()) {
    if (t.name != "bigint" && t.name != "timestamp") ensureCustomTypeFile(t);
  }

  struct Found {
    std::string relative;
    std::string text;
  };
  std::vector<Found> files;
  for (const auto& root : protoLocations) {
    if (!fs::exists(root)) throw SchemaError("proto location does not exist: " + root);
    std::vector<fs::path> paths;
    for (const auto& entry : fs::recursive_directory_iterator(root)) {
      if (entry.is_regular_file() && entry.path().extension() == ".proto") paths.push_back(entry.path());
    }
    std::sort(paths.begin(), paths.end());
    for (const auto& p : paths) {
      std::ifstream in(p, std::ios::binary);
      std::stringstream buf;
      buf << in.rdbuf();
      files.push_back(Found{fs::relative(p, root).generic_string(), buf.str()});
    }
  }
  if (!files.empty()) Logger::info("loading " + std::to_string(files.size()) + " proto file(s)");

  // Parse everything first, so files can import each other in any order.
  std::vector<gpb::FileDescriptorProto> parsed;
  for (const auto& f : files) {
    gpb::io::ArrayInputStream input(f.text.data(), static_cast<int>(f.text.size()));
    CollectingErrors errors(f.relative);
    gpb::io::Tokenizer tokenizer(&input, &errors);
    gpb::compiler::Parser parser;
    parser.RecordErrorsTo(&errors);
    gpb::FileDescriptorProto file;
    if (!parser.Parse(&tokenizer, &file) || !errors.text().empty()) {
      throw SchemaError("failed to parse " + f.relative + ": " + errors.text());
    }
    file.set_name(f.relative);
    injectCustomTypes(file);
    parsed.push_back(std::move(file));
    fresh->parsedSchemas.insert(f.text);
  }
  for (auto& file : parsed) {
    const std::string name = file.name();
    addFile(std::move(file), name);
  }
  Logger::debug("message factory initialized");
}

void MessageFactory::parse(const std::string& proto, const std::string& moduleName) {
  auto st = state();
  if (!st) {
    throw NotInitializedError("cannot parse schema" + (moduleName.empty() ? "" : " for " + moduleName) +
                              " before MessageFactory::init() has run: there is no pool to parse into. Call "
                              "Context::init() (or MessageFactory::init()) first.");
  }
  if (!moduleName.empty() && hasService(moduleName)) {
    Logger::debug("schema for " + moduleName + " already registered, skipping");
    return;
  }
  {
    std::lock_guard<std::mutex> lock(st->buildMutex);
    if (st->parsedSchemas.count(proto)) {
      Logger::debug("schema text already registered" + (moduleName.empty() ? "" : " (as " + moduleName + ")") +
                    ", skipping");
      return;
    }
  }
  const std::string label = moduleName.empty() ? "schema" : moduleName;
  gpb::io::ArrayInputStream input(proto.data(), static_cast<int>(proto.size()));
  CollectingErrors errors(label);
  gpb::io::Tokenizer tokenizer(&input, &errors);
  gpb::compiler::Parser parser;
  parser.RecordErrorsTo(&errors);
  gpb::FileDescriptorProto file;
  if (!parser.Parse(&tokenizer, &file) || !errors.text().empty()) {
    throw SchemaError("failed to parse " + label + ": " + errors.text());
  }
  file.set_name("protobus/parsed/" + std::to_string(std::hash<std::string>{}(proto)) + "/" +
                (moduleName.empty() ? std::string("schema") : moduleName) + ".proto");
  injectCustomTypes(file);
  addFile(std::move(file), label);
  std::lock_guard<std::mutex> lock(st->buildMutex);
  st->parsedSchemas.insert(proto);
}

void MessageFactory::addFile(gpb::FileDescriptorProto file, const std::string& label) {
  auto st = state();
  std::lock_guard<std::mutex> lock(st->buildMutex);
  // A schema whose every type is already known (compiled in, or loaded
  // under another name) is the same schema arriving twice: skip it.
  const auto symbols = topLevelSymbols(file);
  if (!symbols.empty()) {
    bool all = true;
    bool any = false;
    for (const auto& s : symbols) {
      const bool known = st->pool->FindFileContainingSymbol(s) != nullptr;
      all = all && known;
      any = any || known;
    }
    if (all) {
      Logger::debug("schema " + label + " is already loaded, skipping");
      return;
    }
    if (any) {
      std::string present;
      for (const auto& s : symbols) {
        if (st->pool->FindFileContainingSymbol(s) != nullptr) present += (present.empty() ? "" : ", ") + s;
      }
      throw SchemaError("schema " + label + " redefines types that are already loaded (" + present +
                        "); a schema is added once, or every one of its types is new");
    }
  }
  if (st->pool->FindFileByName(file.name()) != nullptr) return;
  const std::string name = file.name();
  if (!st->runtime.Add(file)) throw SchemaError("schema " + label + " could not be added (duplicate file name?)");
  st->runtimeFiles.insert(name);
  if (st->pool->FindFileByName(name) == nullptr) {
    throw SchemaError("schema " + label + " does not build: " + st->errors.take());
  }
}

// Shared schemas use the custom types without importing them. Add the
// import of every custom type a file refers to and does not declare.
void MessageFactory::injectCustomTypes(gpb::FileDescriptorProto& file) {
  std::set<std::string> referenced;
  std::set<std::string> declared;
  const std::string scope = file.package().empty() ? "" : file.package() + ".";
  for (const auto& m : file.message_type()) referencedTypes(m, referenced, declared, scope);
  for (const auto& svc : file.service()) {
    for (const auto& method : svc.method()) {
      referenced.insert(method.input_type());
      referenced.insert(method.output_type());
    }
  }
  auto imports = [&](const std::string& dep) {
    return std::find(file.dependency().begin(), file.dependency().end(), dep) != file.dependency().end();
  };
  for (const auto& t : getCustomTypes()) {
    const bool used = referenced.count(t.name) > 0 || referenced.count("." + t.name) > 0;
    const bool local = declared.count(t.name) > 0 && file.package().empty();
    if (!used || local) continue;
    const std::string dep = (t.name == "bigint" || t.name == "timestamp") ? kTypesFile : customTypeFile(t.name);
    if (dep != kTypesFile) ensureCustomTypeFile(t);
    if (!imports(dep)) file.add_dependency(dep);
  }
}

void MessageFactory::ensureCustomTypeFile(const CustomType& type) {
  auto st = state();
  if (!st) return;
  const std::string name = customTypeFile(type.name);
  std::lock_guard<std::mutex> lock(st->buildMutex);
  if (st->runtimeFiles.count(name)) return;
  if (st->pool->FindFileContainingSymbol(type.name) != nullptr) return;
  gpb::FileDescriptorProto file;
  file.set_name(name);
  file.set_syntax("proto3");
  auto* msg = file.add_message_type();
  msg->set_name(type.name);
  auto* field = msg->add_field();
  field->set_name("value");
  field->set_number(1);
  field->set_label(gpb::FieldDescriptorProto::LABEL_OPTIONAL);
  field->set_proto3_optional(true);
  field->set_oneof_index(0);
  msg->add_oneof_decl()->set_name("_value");
  switch (type.wireType) {
    case CustomWireType::Bytes:
      field->set_type(gpb::FieldDescriptorProto::TYPE_BYTES);
      break;
    case CustomWireType::Int64:
      field->set_type(gpb::FieldDescriptorProto::TYPE_INT64);
      break;
    case CustomWireType::Uint64:
      field->set_type(gpb::FieldDescriptorProto::TYPE_UINT64);
      break;
    case CustomWireType::String:
      field->set_type(gpb::FieldDescriptorProto::TYPE_STRING);
      break;
    case CustomWireType::Int32:
      field->set_type(gpb::FieldDescriptorProto::TYPE_INT32);
      break;
    case CustomWireType::Uint32:
      field->set_type(gpb::FieldDescriptorProto::TYPE_UINT32);
      break;
    case CustomWireType::Double:
      field->set_type(gpb::FieldDescriptorProto::TYPE_DOUBLE);
      break;
  }
  st->runtime.Add(file);
  st->runtimeFiles.insert(name);
  if (st->pool->FindFileByName(name) == nullptr) {
    throw SchemaError("custom type " + type.name + " does not build: " + st->errors.take());
  }
}

void MessageFactory::registerType(const CustomType& type) {
  registerCustomType(type);
  if (type.name == "bigint" || type.name == "timestamp") return;
  ensureCustomTypeFile(type);
}

bool MessageFactory::hasType(const std::string& fullName) const { return findMessageType(fullName) != nullptr; }

bool MessageFactory::hasService(const std::string& fullName) const { return findService(fullName) != nullptr; }

const gpb::ServiceDescriptor* MessageFactory::findService(const std::string& fullName) const {
  auto st = state();
  if (!st) return nullptr;
  std::lock_guard<std::mutex> lock(st->buildMutex);
  return st->pool->FindServiceByName(fullName);
}

const gpb::Descriptor* MessageFactory::findMessageType(const std::string& fullName) const {
  auto st = state();
  if (!st) return nullptr;
  std::lock_guard<std::mutex> lock(st->buildMutex);
  return st->pool->FindMessageTypeByName(fullName);
}

const gpb::MethodDescriptor* MessageFactory::getMethodType(const std::string& methodFullName) const {
  auto [serviceName, methodName] = splitMethodName(methodFullName);
  const auto* service = findService(serviceName);
  if (service == nullptr) throw UnknownMethodError("no service '" + serviceName + "' is loaded");
  const auto* method = service->FindMethodByName(methodName);
  if (method == nullptr) {
    throw UnknownMethodError("service '" + serviceName + "' declares no method '" + methodName + "'");
  }
  return method;
}

std::vector<std::string> MessageFactory::getServiceMethodNames(const std::string& serviceFullName) const {
  const auto* service = findService(serviceFullName);
  if (service == nullptr) throw UnknownMethodError("no service '" + serviceFullName + "' is loaded");
  std::vector<std::string> out;
  for (int i = 0; i < service->method_count(); ++i) out.push_back(std::string(service->method(i)->name()));
  return out;
}

bool MessageFactory::isStreamingMethod(const std::string& methodFullName) const {
  try {
    return getMethodType(methodFullName)->server_streaming();
  } catch (const std::exception& e) {
    Logger::debug("isStreamingMethod(" + methodFullName + "): treating as unary (" + e.what() + ")");
    return false;
  }
}

std::unique_ptr<gpb::Message> MessageFactory::newMessage(const std::string& typeFullName) const {
  auto st = state();
  if (!st) throw NotInitializedError("message factory not initialized");
  if (typeFullName.empty()) throw MessageTypeRequiredError();
  const gpb::Descriptor* d;
  {
    std::lock_guard<std::mutex> lock(st->buildMutex);
    d = st->pool->FindMessageTypeByName(typeFullName);
  }
  if (d == nullptr) throw UnknownMethodError("no message type '" + typeFullName + "' is loaded");
  return std::unique_ptr<gpb::Message>(st->dynamic->GetPrototype(d)->New());
}

std::unique_ptr<gpb::Message> MessageFactory::decodeMessage(const std::string& typeFullName,
                                                           const std::string& data) const {
  auto m = newMessage(typeFullName);
  if (!m->ParseFromString(data)) {
    // Type name and size only: message bodies carry credentials and personal
    // data.
    Logger::error("error decoding message " + typeFullName + " (" + std::to_string(data.size()) + " bytes)");
    throw InvalidMessageError("message did not decode as " + typeFullName);
  }
  validateCustomTypes(*m);
  return m;
}

std::string MessageFactory::encodeMessage(const gpb::Message& message) {
  validateCustomTypes(message);
  std::string out;
  if (!message.SerializeToString(&out)) {
    throw InvalidMessageError("message of type " + std::string(message.GetDescriptor()->full_name()) +
                              " did not encode");
  }
  return out;
}

std::string MessageFactory::buildRequest(const std::string& methodFullName, const gpb::Message& obj,
                                         const std::string& actor) const {
  const auto* method = getMethodType(methodFullName);
  const std::string want(method->input_type()->full_name());
  const std::string got(obj.GetDescriptor()->full_name());
  if (want != got) {
    throw InvalidRequestError("request for " + methodFullName + " must be a " + want + ", got a " + got);
  }
  wire::Request r;
  r.method = methodFullName;
  r.actor = actor;
  r.data = encodeMessage(obj);
  return wire::encodeRequest(r);
}

RequestEnvelope MessageFactory::decodeRequestEnvelope(const std::string& data) {
  auto r = wire::decodeRequest(data);
  return RequestEnvelope{std::move(r.method), std::move(r.actor), std::move(r.data)};
}

std::unique_ptr<gpb::Message> MessageFactory::decodeRequestPayload(const std::string& methodFullName,
                                                                  const std::string& payload) const {
  const auto* method = getMethodType(methodFullName);
  return decodeMessage(std::string(method->input_type()->full_name()), payload);
}

std::string MessageFactory::buildResponse(const std::string& methodFullName, const gpb::Message& result) const {
  const auto* method = getMethodType(methodFullName);
  const std::string want(method->output_type()->full_name());
  const std::string got(result.GetDescriptor()->full_name());
  if (want != got) {
    throw InvalidResultError("result of " + methodFullName + " must be a " + want + ", got a " + got);
  }
  return buildResultResponse(methodFullName, encodeMessage(result));
}

std::string MessageFactory::buildResultResponse(const std::string& methodFullName, const std::string& encodedResult) {
  wire::Response r;
  r.result = wire::Result{methodFullName, encodedResult};
  return wire::encodeResponse(r);
}

std::string MessageFactory::buildErrorResponse(const std::string& methodFullName, const std::string& message,
                                               const std::string& code) {
  wire::Response r;
  r.error = wire::ResponseError{methodFullName, message, code};
  return wire::encodeResponse(r);
}

DecodedResponse MessageFactory::decodeResponse(const std::string& data) {
  auto r = wire::decodeResponse(data);
  DecodedResponse out;
  if (r.error) {
    out.errorMethod = r.error->method;
    out.errorMessage = r.error->message;
    out.errorCode = r.error->code;
    out.method = r.error->method;
  } else {
    out.method = r.result->method;
    out.data = std::move(r.result->data);
  }
  return out;
}

std::string MessageFactory::buildEvent(const std::string& type, const gpb::Message& obj,
                                       const std::string& topic) const {
  const std::string got(obj.GetDescriptor()->full_name());
  if (type != got) throw InvalidMessageError("event declared as " + type + " is a " + got);
  wire::Event e;
  e.type = type;
  e.topic = topic;
  e.data = encodeMessage(obj);
  return wire::encodeEvent(e);
}

DecodedEvent MessageFactory::decodeEvent(const std::string& data) const {
  auto e = wire::decodeEvent(data);
  DecodedEvent out;
  out.type = std::move(e.type);
  out.topic = std::move(e.topic);
  out.data = std::move(e.data);
  out.message = decodeMessage(out.type, out.data);
  return out;
}

}  // namespace protobus
