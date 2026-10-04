// protoc-gen-protobus-cpp: the protobus code generator as a protoc plugin,
// for builds that already run protoc or buf.
//
//   protoc --cpp_out=gen --protobus-cpp_out=gen -I proto -I <protobus include dir> proto/*.proto
//
// Schemas compiled this way import the custom types themselves
// (`import "protobus/types.proto";`); the protobus-cpp CLI adds that import
// for you.
#include <google/protobuf/compiler/plugin.h>

#include "generator.h"

int main(int argc, char** argv) {
  protobus::codegen::ProtobusGenerator generator;
  return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
