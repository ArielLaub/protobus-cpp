// The protobus C++ code generator, shared by protoc-gen-protobus-cpp and the
// protobus-cpp CLI.
//
// For every `service S` in a .proto it emits, next to protoc's X.pb.h:
//
//   class SBase  : protobus::RunnableService  - a virtual method per rpc to
//                                               override; the constructor
//                                               registers them all
//   class SProxy                               - a typed client, a method per
//                                               rpc
//
// in X.protobus.h / X.protobus.cc, in the namespace of the file's package.
#pragma once

#include <google/protobuf/compiler/code_generator.h>

#include <string>

namespace protobus::codegen {

class ProtobusGenerator : public google::protobuf::compiler::CodeGenerator {
 public:
  bool Generate(const google::protobuf::FileDescriptor* file, const std::string& parameter,
                google::protobuf::compiler::GeneratorContext* context, std::string* error) const override;

  uint64_t GetSupportedFeatures() const override { return FEATURE_PROTO3_OPTIONAL; }
};

// The C++ name a proto method gets: its own, unless that is a C++ keyword or
// collides with a member of the generated classes' bases, when `_` is
// appended. Routing always uses the proto name.
std::string methodIdentifier(const std::string& protoName);

// "pkg.sub" -> "pkg::sub".
std::string packageNamespace(const std::string& package);

}  // namespace protobus::codegen
