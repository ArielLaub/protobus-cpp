// Custom types: scalar-looking protobuf types with a fixed wire format,
// shared by every protobus port.
//
// The built-ins are `bigint` (an unsigned integer up to 2^256-1, carried as
// exactly 32 big-endian bytes) and `timestamp` (signed milliseconds since the
// epoch). Both are one-field messages declared at the root of the type
// namespace, so a shared schema writes `bigint amount = 1;` and every port
// reads the same bytes. In C++ they are the generated classes ::bigint and
// ::timestamp (from <protobus/types.pb.h>); the helpers below convert them to
// and from Uint256 and std::chrono time points.
//
// Further custom types can be declared with registerCustomType(). Each is a
// root-level message `<name> { optional <wireType> value = 1; }`; the
// MessageFactory and the protobus-cpp CLI add its definition to any schema
// that uses it.
#pragma once

#include <array>
#include <chrono>
#include <compare>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "protobus/types.pb.h"

namespace google::protobuf {
class Message;
class Descriptor;
}  // namespace google::protobuf

namespace protobus {

// Width of the bigint wire format, in bytes.
inline constexpr size_t kBigintBytes = 32;

// An unsigned 256-bit integer: the in-language value of a `bigint`.
//
// Construction refuses anything outside 0..2^256-1 with CustomTypeRangeError
// rather than coercing it: for financial and on-chain amounts, failing loudly
// is the only safe behaviour.
class Uint256 {
 public:
  constexpr Uint256() = default;
  constexpr Uint256(uint64_t v) : limbs_{v, 0, 0, 0} {}  // NOLINT(google-explicit-constructor)

  // A decimal string, or hexadecimal with a 0x prefix. Throws
  // CustomTypeRangeError for a negative, malformed or oversized value.
  static Uint256 parse(std::string_view text);
  // Big-endian bytes, at most 32. Shorter input is zero-extended on the left.
  static Uint256 fromBytes(std::string_view bigEndian);

  // Exactly 32 big-endian bytes.
  std::string toBytes() const;
  std::string toString() const;  // decimal
  std::string toHex() const;     // 0x-prefixed, lowercase, no leading zeros

  bool fitsUint64() const { return limbs_[1] == 0 && limbs_[2] == 0 && limbs_[3] == 0; }
  // Throws CustomTypeRangeError when the value does not fit.
  uint64_t toUint64() const;

  // Arithmetic is checked: overflow and underflow throw CustomTypeRangeError.
  Uint256 operator+(const Uint256& o) const;
  Uint256 operator-(const Uint256& o) const;
  Uint256 operator*(const Uint256& o) const;
  Uint256& operator+=(const Uint256& o) { return *this = *this + o; }
  Uint256& operator-=(const Uint256& o) { return *this = *this - o; }

  friend bool operator==(const Uint256& a, const Uint256& b) = default;
  friend std::strong_ordering operator<=>(const Uint256& a, const Uint256& b);

  static Uint256 max();

 private:
  // Little-endian 64-bit limbs.
  std::array<uint64_t, 4> limbs_{0, 0, 0, 0};
};

// ---- bigint ------------------------------------------------------------------

// A ::bigint holding v, as exactly 32 bytes.
::bigint makeBigint(const Uint256& v);
// Parse a decimal or 0x-hex string into a ::bigint.
::bigint makeBigint(std::string_view text);
// Set an existing (e.g. mutable_*) ::bigint.
void setBigint(::bigint* target, const Uint256& v);
// The value of a ::bigint. An unset or empty value is 0. Throws
// CustomTypeRangeError for a wire value wider than 32 bytes.
Uint256 toUint256(const ::bigint& b);

// ---- timestamp -----------------------------------------------------------------

using TimePoint = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>;

// The largest magnitude, in milliseconds, every port can represent (the
// range of an ECMAScript Date).
inline constexpr int64_t kMaxTimestampMs = 8640000000000000LL;

// A ::timestamp holding t at millisecond precision. Throws
// CustomTypeRangeError beyond +/- kMaxTimestampMs.
::timestamp makeTimestamp(std::chrono::system_clock::time_point t);
::timestamp makeTimestampMs(int64_t millisSinceEpoch);
void setTimestamp(::timestamp* target, std::chrono::system_clock::time_point t);
TimePoint toTimePoint(const ::timestamp& ts);
int64_t toMillis(const ::timestamp& ts);

// ---- the custom-type registry --------------------------------------------------

// The wire type a custom type's single `value` field is declared with.
enum class CustomWireType { Bytes, Int64, Uint64, String, Int32, Uint32, Double };

const char* toString(CustomWireType t);

struct CustomType {
  // As it appears in .proto files, e.g. `uuid`. A root-level name.
  std::string name;
  CustomWireType wireType = CustomWireType::Bytes;
};

// Register a custom type, process-wide. Idempotent for an identical
// definition; a name re-registered with a different wire type throws
// CustomTypeConflictError, because schemas already built against the first
// definition would keep encoding in its format.
void registerCustomType(const CustomType& type);
bool isCustomType(const std::string& name);
std::vector<CustomType> getCustomTypes();
// The .proto source declaring every registered custom type except the
// built-ins, which live in protobus/types.proto.
std::string customTypesProto();

// ---- validation ----------------------------------------------------------------

// Check every built-in custom type value in `message` against its wire format:
// a bigint is at most 32 bytes. Messages that cannot contain a bigint are
// skipped after one cached descriptor walk. Throws CustomTypeRangeError.
void validateCustomTypes(const google::protobuf::Message& message);

}  // namespace protobus
