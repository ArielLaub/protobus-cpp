// The protobus envelopes: the five small protobuf messages every request,
// reply and event travels inside.
//
// The envelopes have no .proto file and no package. In the TypeScript
// reference they are protobufjs decorator classes, which means presence works
// proto2-style: a field set to an empty string or empty bytes is still
// written. This module reproduces those bytes exactly rather than leaning on
// generated proto3 code, which would omit them. Both forms decode identically
// on every port, but byte parity keeps golden tests and captured traffic
// comparable across languages.
//
//   message RequestContainer  { string method = 1; string actor = 2; bytes data = 3; }
//   message ResponseResult    { string method = 1; bytes data = 2; }
//   message ResponseError     { string method = 1; string message = 2; string code = 3; }
//   message ResponseContainer { oneof value { ResponseResult result = 1; ResponseError error = 2; } }
//   message EventContainer    { string type = 1; string topic = 2; bytes data = 3; }
#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace protobus::wire {

struct Request {
  // The contract method name, "<package>.<Service>.<method>".
  std::string method;
  // Free-text caller identity, for tracing only. Not authenticated.
  std::string actor;
  // The encoded request message.
  std::string data;
};

struct Result {
  std::string method;
  std::string data;
};

// Only these three fields cross the wire: an error's type, stack and any
// other detail stay in the process that raised it.
struct ResponseError {
  std::string method;
  std::string message;
  std::string code;
};

// Exactly one of result and error is set.
struct Response {
  std::optional<Result> result;
  std::optional<ResponseError> error;
};

struct Event {
  // The fully-qualified protobuf message name of data, without a leading dot.
  std::string type;
  // The topic the event was published under.
  std::string topic;
  std::string data;
};

// Bytes that are not a valid envelope.
class MalformedError : public std::runtime_error {
 public:
  explicit MalformedError(const std::string& what) : std::runtime_error("protobus: malformed envelope: " + what) {}
};

// The method and data are always written; the actor only when there is one,
// as TypeScript does when a caller passes none.
std::string encodeRequest(const Request& r);
// Throws std::invalid_argument unless exactly one member is set.
std::string encodeResponse(const Response& r);
// All three fields are always written.
std::string encodeEvent(const Event& e);
// A bare ResponseResult, not wrapped in a container.
std::string encodeResult(const Result& r);

Request decodeRequest(std::string_view b);
// When both members are present the error wins, matching the TypeScript
// reader, so the same bytes cannot mean success on one port and failure on
// another. A container with neither is refused.
Response decodeResponse(std::string_view b);
Event decodeEvent(std::string_view b);

// Low-level protobuf wire helpers, shared with the custom-type code.
void appendVarint(std::string& out, uint64_t v);
void appendTag(std::string& out, uint32_t field, uint32_t wireType);
void appendBytesField(std::string& out, uint32_t field, std::string_view v);

}  // namespace protobus::wire
