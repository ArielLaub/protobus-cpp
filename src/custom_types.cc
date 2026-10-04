#include "protobus/custom_types.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/reflection.h>

#include <algorithm>
#include <cctype>
#include <map>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "protobus/errors.h"

namespace protobus {

// ---- Uint256 -------------------------------------------------------------------

namespace {

// a * m + carry over the limbs, returning the overflow limb.
uint64_t mulSmall(std::array<uint64_t, 4>& limbs, uint64_t m, uint64_t add) {
  unsigned __int128 carry = add;
  for (auto& limb : limbs) {
    const unsigned __int128 v = static_cast<unsigned __int128>(limb) * m + carry;
    limb = static_cast<uint64_t>(v);
    carry = v >> 64;
  }
  return static_cast<uint64_t>(carry);
}

// Divide in place by d, returning the remainder.
uint64_t divSmall(std::array<uint64_t, 4>& limbs, uint64_t d) {
  unsigned __int128 rem = 0;
  for (int i = 3; i >= 0; --i) {
    const unsigned __int128 cur = (rem << 64) | limbs[i];
    limbs[i] = static_cast<uint64_t>(cur / d);
    rem = cur % d;
  }
  return static_cast<uint64_t>(rem);
}

bool isZero(const std::array<uint64_t, 4>& l) { return l[0] == 0 && l[1] == 0 && l[2] == 0 && l[3] == 0; }

[[noreturn]] void rangeError(const std::string& what) { throw CustomTypeRangeError(what); }

}  // namespace

Uint256 Uint256::max() {
  Uint256 v;
  v.limbs_ = {~0ULL, ~0ULL, ~0ULL, ~0ULL};
  return v;
}

Uint256 Uint256::parse(std::string_view text) {
  std::string_view s = text;
  while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.remove_prefix(1);
  while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.remove_suffix(1);
  if (s.empty()) rangeError("bigint value is empty");
  if (s.front() == '-') {
    rangeError("bigint value " + std::string(text) +
               " is negative; the protobus bigint wire format is unsigned (0 .. 2^256-1)");
  }
  if (s.front() == '+') s.remove_prefix(1);
  Uint256 v;
  if (s.size() > 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) {
    s.remove_prefix(2);
    for (char c : s) {
      int d;
      if (c >= '0' && c <= '9') {
        d = c - '0';
      } else if (c >= 'a' && c <= 'f') {
        d = c - 'a' + 10;
      } else if (c >= 'A' && c <= 'F') {
        d = c - 'A' + 10;
      } else {
        rangeError("bigint value " + std::string(text) + " is not a hexadecimal integer");
      }
      if (mulSmall(v.limbs_, 16, static_cast<uint64_t>(d)) != 0) {
        rangeError("bigint value " + std::string(text) + " exceeds the maximum representable value 2^256-1");
      }
    }
    return v;
  }
  for (char c : s) {
    if (c < '0' || c > '9') rangeError("bigint value " + std::string(text) + " is not a decimal integer");
    if (mulSmall(v.limbs_, 10, static_cast<uint64_t>(c - '0')) != 0) {
      rangeError("bigint value " + std::string(text) + " exceeds the maximum representable value 2^256-1");
    }
  }
  return v;
}

Uint256 Uint256::fromBytes(std::string_view b) {
  if (b.size() > kBigintBytes) {
    rangeError("bigint wire value is " + std::to_string(b.size()) +
               " bytes; the protobus bigint wire format is at most " + std::to_string(kBigintBytes));
  }
  Uint256 v;
  for (unsigned char c : b) mulSmall(v.limbs_, 256, c);
  return v;
}

std::string Uint256::toBytes() const {
  std::string out(kBigintBytes, '\0');
  for (size_t i = 0; i < kBigintBytes; ++i) {
    const size_t limb = i / 8;
    const size_t shift = (i % 8) * 8;
    out[kBigintBytes - 1 - i] = static_cast<char>((limbs_[limb] >> shift) & 0xff);
  }
  return out;
}

std::string Uint256::toString() const {
  if (isZero(limbs_)) return "0";
  auto l = limbs_;
  std::string out;
  while (!isZero(l)) out.push_back(static_cast<char>('0' + divSmall(l, 10)));
  std::reverse(out.begin(), out.end());
  return out;
}

std::string Uint256::toHex() const {
  static const char* hex = "0123456789abcdef";
  std::string out;
  for (int i = 3; i >= 0; --i) {
    for (int s = 60; s >= 0; s -= 4) {
      const int d = static_cast<int>((limbs_[i] >> s) & 0xf);
      if (out.empty() && d == 0) continue;
      out.push_back(hex[d]);
    }
  }
  return "0x" + (out.empty() ? std::string("0") : out);
}

uint64_t Uint256::toUint64() const {
  if (!fitsUint64()) rangeError("bigint value " + toString() + " does not fit in 64 bits");
  return limbs_[0];
}

Uint256 Uint256::operator+(const Uint256& o) const {
  Uint256 r;
  unsigned __int128 carry = 0;
  for (size_t i = 0; i < 4; ++i) {
    const unsigned __int128 v = static_cast<unsigned __int128>(limbs_[i]) + o.limbs_[i] + carry;
    r.limbs_[i] = static_cast<uint64_t>(v);
    carry = v >> 64;
  }
  if (carry != 0) rangeError("bigint addition overflows 2^256-1");
  return r;
}

Uint256 Uint256::operator-(const Uint256& o) const {
  if (*this < o) rangeError("bigint subtraction would go below zero");
  Uint256 r;
  uint64_t borrow = 0;
  for (size_t i = 0; i < 4; ++i) {
    const uint64_t a = limbs_[i];
    const uint64_t b = o.limbs_[i];
    r.limbs_[i] = a - b - borrow;
    borrow = (a < b || (a == b && borrow)) ? 1 : 0;
  }
  return r;
}

Uint256 Uint256::operator*(const Uint256& o) const {
  std::array<uint64_t, 8> wide{};
  for (size_t i = 0; i < 4; ++i) {
    unsigned __int128 carry = 0;
    for (size_t j = 0; j < 4; ++j) {
      const unsigned __int128 v =
          static_cast<unsigned __int128>(limbs_[i]) * o.limbs_[j] + wide[i + j] + carry;
      wide[i + j] = static_cast<uint64_t>(v);
      carry = v >> 64;
    }
    wide[i + 4] = static_cast<uint64_t>(carry);
  }
  for (size_t k = 4; k < 8; ++k) {
    if (wide[k] != 0) rangeError("bigint multiplication overflows 2^256-1");
  }
  Uint256 r;
  for (size_t k = 0; k < 4; ++k) r.limbs_[k] = wide[k];
  return r;
}

std::strong_ordering operator<=>(const Uint256& a, const Uint256& b) {
  for (int i = 3; i >= 0; --i) {
    if (a.limbs_[i] != b.limbs_[i]) return a.limbs_[i] <=> b.limbs_[i];
  }
  return std::strong_ordering::equal;
}

// ---- bigint ------------------------------------------------------------------

::bigint makeBigint(const Uint256& v) {
  ::bigint b;
  b.set_value(v.toBytes());
  return b;
}

::bigint makeBigint(std::string_view text) { return makeBigint(Uint256::parse(text)); }

void setBigint(::bigint* target, const Uint256& v) { target->set_value(v.toBytes()); }

Uint256 toUint256(const ::bigint& b) { return Uint256::fromBytes(b.value()); }

// ---- timestamp -----------------------------------------------------------------

namespace {
void checkTimestamp(int64_t ms) {
  if (ms > kMaxTimestampMs || ms < -kMaxTimestampMs) {
    rangeError("timestamp value " + std::to_string(ms) +
               " ms is not a valid instant; every port accepts at most +/-8.64e15 ms around the epoch");
  }
}
}  // namespace

::timestamp makeTimestampMs(int64_t ms) {
  checkTimestamp(ms);
  ::timestamp t;
  t.set_value(ms);
  return t;
}

::timestamp makeTimestamp(std::chrono::system_clock::time_point t) {
  return makeTimestampMs(std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count());
}

void setTimestamp(::timestamp* target, std::chrono::system_clock::time_point t) { *target = makeTimestamp(t); }

int64_t toMillis(const ::timestamp& ts) { return ts.value(); }

TimePoint toTimePoint(const ::timestamp& ts) { return TimePoint(std::chrono::milliseconds(ts.value())); }

// ---- the custom-type registry --------------------------------------------------

const char* toString(CustomWireType t) {
  switch (t) {
    case CustomWireType::Bytes:
      return "bytes";
    case CustomWireType::Int64:
      return "int64";
    case CustomWireType::Uint64:
      return "uint64";
    case CustomWireType::String:
      return "string";
    case CustomWireType::Int32:
      return "int32";
    case CustomWireType::Uint32:
      return "uint32";
    case CustomWireType::Double:
      return "double";
  }
  return "bytes";
}

namespace {

std::mutex registryMutex;
std::map<std::string, CustomType>& registry() {
  static std::map<std::string, CustomType> r = {
      {"bigint", CustomType{"bigint", CustomWireType::Bytes}},
      {"timestamp", CustomType{"timestamp", CustomWireType::Int64}},
  };
  return r;
}

bool validName(const std::string& name) {
  if (name.empty()) return false;
  if (!(std::isalpha(static_cast<unsigned char>(name[0])) || name[0] == '_')) return false;
  return std::all_of(name.begin(), name.end(),
                     [](unsigned char c) { return std::isalnum(c) || c == '_'; });
}

}  // namespace

void registerCustomType(const CustomType& type) {
  if (!validName(type.name)) {
    throw CustomTypeConflictError("custom type name '" + type.name +
                                  "' is not a valid root-level protobuf identifier");
  }
  std::lock_guard<std::mutex> lock(registryMutex);
  auto it = registry().find(type.name);
  if (it != registry().end()) {
    if (it->second.wireType != type.wireType) {
      throw CustomTypeConflictError("custom type '" + type.name + "' is already registered with wire type '" +
                                    toString(it->second.wireType) + "'; re-registering it as '" +
                                    toString(type.wireType) +
                                    "' would keep encoding in the original wire format. Use a different name, "
                                    "or keep the original wire type.");
    }
    return;
  }
  registry().emplace(type.name, type);
}

bool isCustomType(const std::string& name) {
  std::lock_guard<std::mutex> lock(registryMutex);
  return registry().count(name) > 0;
}

std::vector<CustomType> getCustomTypes() {
  std::lock_guard<std::mutex> lock(registryMutex);
  std::vector<CustomType> out;
  for (const auto& [_, t] : registry()) out.push_back(t);
  return out;
}

std::string customTypesProto() {
  std::string out = "syntax = \"proto3\";\n";
  for (const auto& t : getCustomTypes()) {
    if (t.name == "bigint" || t.name == "timestamp") continue;
    out += "message " + t.name + " {\n  optional " + toString(t.wireType) + " value = 1;\n}\n";
  }
  return out;
}

// ---- validation ----------------------------------------------------------------

namespace {

using google::protobuf::Descriptor;
using google::protobuf::FieldDescriptor;
using google::protobuf::Message;
using google::protobuf::Reflection;

bool isBigint(const Descriptor* d) { return d->full_name() == "bigint"; }

std::mutex reachMutex;
std::unordered_map<const Descriptor*, bool>& reachCache() {
  static std::unordered_map<const Descriptor*, bool> c;
  return c;
}

bool reachesBigintWalk(const Descriptor* d, std::unordered_set<const Descriptor*>& visiting) {
  if (isBigint(d)) return true;
  if (!visiting.insert(d).second) return false;  // a cycle; the other paths decide
  for (int i = 0; i < d->field_count(); ++i) {
    const FieldDescriptor* f = d->field(i);
    if (f->is_map()) f = f->message_type()->map_value();
    if (f->message_type() != nullptr && reachesBigintWalk(f->message_type(), visiting)) return true;
  }
  return false;
}

bool reachesBigint(const Descriptor* d) {
  {
    std::lock_guard<std::mutex> lock(reachMutex);
    auto it = reachCache().find(d);
    if (it != reachCache().end()) return it->second;
  }
  std::unordered_set<const Descriptor*> visiting;
  const bool found = reachesBigintWalk(d, visiting);
  std::lock_guard<std::mutex> lock(reachMutex);
  reachCache()[d] = found;
  return found;
}

void checkMessage(const Message& m) {
  const Descriptor* d = m.GetDescriptor();
  const Reflection* r = m.GetReflection();
  if (isBigint(d)) {
    const FieldDescriptor* value = d->FindFieldByNumber(1);
    if (value != nullptr && value->type() == FieldDescriptor::TYPE_BYTES) {
      std::string scratch;
      const std::string& bytes = r->GetStringReference(m, value, &scratch);
      if (bytes.size() > kBigintBytes) {
        rangeError("bigint wire value is " + std::to_string(bytes.size()) + " bytes, at most " +
                   std::to_string(kBigintBytes) + " allowed");
      }
    }
    return;
  }
  std::vector<const FieldDescriptor*> fields;
  r->ListFields(m, &fields);
  for (const FieldDescriptor* f : fields) {
    if (f->message_type() == nullptr) continue;
    if (f->is_map()) {
      const FieldDescriptor* valueField = f->message_type()->map_value();
      if (valueField->message_type() == nullptr || !reachesBigint(valueField->message_type())) continue;
      const int n = r->FieldSize(m, f);
      for (int i = 0; i < n; ++i) {
        const Message& entry = r->GetRepeatedMessage(m, f, i);
        const Reflection* er = entry.GetReflection();
        if (er->HasField(entry, valueField)) checkMessage(er->GetMessage(entry, valueField));
      }
      continue;
    }
    if (!reachesBigint(f->message_type())) continue;
    if (f->is_repeated()) {
      const int n = r->FieldSize(m, f);
      for (int i = 0; i < n; ++i) checkMessage(r->GetRepeatedMessage(m, f, i));
    } else {
      checkMessage(r->GetMessage(m, f));
    }
  }
}

}  // namespace

void validateCustomTypes(const Message& message) {
  if (!reachesBigint(message.GetDescriptor())) return;
  checkMessage(message);
}

}  // namespace protobus
