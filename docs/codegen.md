# Code generation

Schemas are shared verbatim between the four ports. They need no C++ options
and no import for the custom types.

## `protobus_generate()`

The CMake package provides one function:

```cmake
find_package(protobus CONFIG REQUIRED)
add_executable(app main.cc)
protobus_generate(TARGET app PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto
                  [OUT_DIR <dir>] [CUSTOM_TYPES uuid:bytes ...])
target_link_libraries(app PRIVATE protobus::protobus)
```

At build time it runs the `protobus-cpp` CLI over every `.proto` under
`PROTO_DIR`, adds the generated sources to the target and puts the output
directory on its include path. A file `billing/invoice.proto` produces
`billing/invoice.pb.h` (protoc's messages) and `billing/invoice.protobus.h`
(the service bases and proxies).

## The CLI

```
protobus-cpp generate [--proto-dir DIR]... [--out DIR] [--custom-type NAME:WIRE]... [--protoc PATH]
protobus-cpp generate:service <Name> [--proto-dir DIR] [--services-dir DIR]
protobus-cpp init
```

`generate` copies the schemas to a staging directory, adds
`import "protobus/types.proto";` to each that uses `bigint` or `timestamp`
without declaring it (and the import of each `--custom-type` it uses), then
runs protoc with `--cpp_out` and the protobus plugin. protoc is `--protoc`,
then `$PROTOC`, then the one on the `PATH`; use the protoc of the protobuf you
link against.

`generate:service Calculator` reads `Calculator.proto` and writes
`services/calculator/CalculatorService.cc`: a class per service overriding
every rpc with a stub, and a `main` that starts them with `RunnableService`.
It refuses to overwrite a file, and a name that could reach outside the
services directory.

## What is generated

For `service Service` in `package Calculator`, in namespace `Calculator`:

```cpp
class ServiceBase : public ::protobus::RunnableService {
 public:
  static constexpr const char* kServiceName = "Calculator.Service";
  explicit ServiceBase(::protobus::Context& context, ::protobus::MessageServiceOptions options = {});
  std::string ServiceName() const override { return kServiceName; }

  // rpc add(Calculator.AddRequest) returns (Calculator.AddResponse);
  virtual ::Calculator::AddResponse add(const ::Calculator::AddRequest& request, ::protobus::CallContext& context);
  // ...
};

class ServiceProxy {
 public:
  static constexpr const char* kServiceName = "Calculator.Service";
  explicit ServiceProxy(::protobus::Context& context, std::string serviceName = kServiceName);
  void init();
  ::protobus::ServiceProxy& raw();

  ::Calculator::AddResponse add(const ::Calculator::AddRequest& request, const ::protobus::CallOptions& options = {});
  // ...
};
```

A dotted package becomes nested namespaces (`deep.pkg` → `deep::pkg`). An rpc
named like a C++ keyword or a base-class member gets a trailing underscore in
C++ (`delete` → `delete_`, `init` → `init_`). The base's constructor registers
every rpc, decoding each request into its generated type and checking its
custom-type values before the handler runs.

## With protoc or buf directly

`protoc-gen-protobus-cpp` is the same generator as a protoc plugin:

```bash
protoc -I proto -I /usr/local/include \
  --cpp_out=gen --protobus-cpp_out=gen \
  --plugin=protoc-gen-protobus-cpp=$(which protoc-gen-protobus-cpp) \
  proto/Calculator.proto
```

Schemas compiled this way import the custom types themselves:
`import "protobus/types.proto";`, found in protobus's install include
directory. The `protobus/types.pb.h` it refers to ships with the library: do
not generate it again.

## Custom types

`bigint` and `timestamp` are declared at the root of the type namespace, as
in every port:

```protobuf
message bigint    { optional bytes value = 1; }  // unsigned, up to 2^256-1: 32 bytes, big-endian
message timestamp { optional int64 value = 1; }  // signed milliseconds since the epoch
```

In C++ they are `::bigint` and `::timestamp`, with helpers in
`<protobus/custom_types.h>`:

<!-- doc-check: compile proto=crosslang/proto -->
```cpp
#include <chrono>

#include "interop.pb.h"
#include <protobus/custom_types.h>

void amounts(interop::Balance& balance) {
  *balance.mutable_amount() = protobus::makeBigint("1000000000000000000000000000000");
  protobus::Uint256 amount = protobus::toUint256(balance.amount());
  *balance.mutable_amount() = protobus::makeBigint(amount + 1);

  *balance.mutable_as_of() = protobus::makeTimestamp(std::chrono::system_clock::now());
  protobus::TimePoint when = protobus::toTimePoint(balance.as_of());
  int64_t ms = protobus::toMillis(balance.as_of());
  (void)when;
  (void)ms;
}
```

`protobus::Uint256` parses decimal or `0x` hex, renders both, and does checked
arithmetic. Every port refuses a negative or oversized `bigint`, and refuses
to decode one wider than 32 bytes; protobus-cpp checks every request, reply
and event it encodes or decodes (a malformed value in a request is answered
`PROTOCOL_ERROR`). A `timestamp` is refused beyond ±8.64e15 ms, the range
every port can represent.

### Custom types of your own

Declare a type with `--custom-type NAME:WIRE` (`CUSTOM_TYPES` in
`protobus_generate()`), where WIRE is `bytes`, `int64`, `uint64`, `string`,
`int32`, `uint32` or `double`. It becomes a root-level message
`NAME { optional WIRE value = 1; }`, generated into `protobus/custom/NAME.pb.h`
and imported by every schema that uses it. At runtime,
`MessageFactory::registerType({"uuid", protobus::CustomWireType::Bytes})` does
the same for schemas loaded dynamically. The other ports must declare the same
type with the same wire type.

## Loading schemas at runtime

`Context::init(url, {"./proto"})` loads every `.proto` under the directories
into the context's `MessageFactory`, for the dynamic API. Types compiled into
the program are always visible; a schema that is both compiled in and loaded
is recognised as the same and loaded once.
