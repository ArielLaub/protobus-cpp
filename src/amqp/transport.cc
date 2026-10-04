#include "protobus/transport.h"

#include <cmath>
#include <cstdio>

namespace protobus::amqp {

AmqpError::AmqpError(const std::string& message, int replyCode, bool channelClosed)
    : Error(message, "AmqpError"), replyCode_(replyCode), channelClosed_(channelClosed) {}

FieldValue FieldValue::fromBool(bool v) {
  FieldValue f;
  f.kind = Kind::Bool;
  f.boolean = v;
  return f;
}

FieldValue FieldValue::fromInt(int64_t v) {
  FieldValue f;
  f.kind = Kind::I64;
  f.integer = v;
  return f;
}

FieldValue FieldValue::fromInt32(int32_t v) {
  FieldValue f;
  f.kind = Kind::I32;
  f.integer = v;
  return f;
}

FieldValue FieldValue::fromString(std::string v) {
  FieldValue f;
  f.kind = Kind::String;
  f.text = std::move(v);
  return f;
}

FieldValue FieldValue::fromDouble(double v) {
  FieldValue f;
  f.kind = Kind::Double;
  f.real = v;
  return f;
}

FieldValue FieldValue::fromTable(FieldTable v) {
  FieldValue f;
  f.kind = Kind::Table;
  f.table = std::make_shared<FieldTable>(std::move(v));
  return f;
}

FieldValue FieldValue::fromArray(FieldArray v) {
  FieldValue f;
  f.kind = Kind::Array;
  f.array = std::make_shared<FieldArray>(std::move(v));
  return f;
}

namespace {

std::optional<int64_t> parseDecimal(const std::string& s) {
  if (s.empty() || s.size() > 19) return std::nullopt;
  size_t i = 0;
  bool negative = false;
  if (s[0] == '-' || s[0] == '+') {
    negative = s[0] == '-';
    i = 1;
    if (s.size() == 1) return std::nullopt;
  }
  int64_t v = 0;
  for (; i < s.size(); ++i) {
    if (s[i] < '0' || s[i] > '9') return std::nullopt;
    v = v * 10 + (s[i] - '0');
  }
  return negative ? -v : v;
}

}  // namespace

std::optional<int64_t> FieldValue::asInt() const {
  switch (kind) {
    case Kind::I8:
    case Kind::I16:
    case Kind::I32:
    case Kind::I64:
    case Kind::Timestamp:
      return integer;
    case Kind::U8:
    case Kind::U16:
    case Kind::U32:
    case Kind::U64:
      if (unsigned_ > static_cast<uint64_t>(INT64_MAX)) return std::nullopt;
      return static_cast<int64_t>(unsigned_);
    case Kind::Float:
    case Kind::Double:
      if (!std::isfinite(real) || real != std::floor(real) || std::fabs(real) > 9.0e15) return std::nullopt;
      return static_cast<int64_t>(real);
    case Kind::String:
    case Kind::Bytes:
      return parseDecimal(text);
    default:
      return std::nullopt;
  }
}

std::optional<bool> FieldValue::asBool() const {
  switch (kind) {
    case Kind::Bool:
      return boolean;
    case Kind::String:
    case Kind::Bytes: {
      std::string lower;
      for (char c : text) lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
      return lower == "true" || lower == "1";
    }
    case Kind::Void:
      return std::nullopt;
    default: {
      auto i = asInt();
      if (i) return *i != 0;
      if (kind == Kind::Float || kind == Kind::Double) return real != 0;
      return std::nullopt;
    }
  }
}

std::optional<std::string> FieldValue::asString() const {
  switch (kind) {
    case Kind::String:
    case Kind::Bytes:
      return text;
    case Kind::Bool:
      return boolean ? "true" : "false";
    case Kind::Float:
    case Kind::Double: {
      char buf[64];
      std::snprintf(buf, sizeof buf, "%.17g", real);
      return std::string(buf);
    }
    default: {
      auto i = asInt();
      if (i) return std::to_string(*i);
      if (kind == Kind::U64) return std::to_string(unsigned_);
      return std::nullopt;
    }
  }
}

bool FieldValue::operator==(const FieldValue& o) const {
  if (kind != o.kind) return false;
  switch (kind) {
    case Kind::Void:
      return true;
    case Kind::Bool:
      return boolean == o.boolean;
    case Kind::I8:
    case Kind::I16:
    case Kind::I32:
    case Kind::I64:
    case Kind::Timestamp:
      return integer == o.integer;
    case Kind::U8:
    case Kind::U16:
    case Kind::U32:
    case Kind::U64:
      return unsigned_ == o.unsigned_;
    case Kind::Float:
    case Kind::Double:
      return real == o.real;
    case Kind::Decimal:
      return decimalScale == o.decimalScale && decimalValue == o.decimalValue;
    case Kind::String:
    case Kind::Bytes:
      return text == o.text;
    case Kind::Table:
      return (table && o.table) ? *table == *o.table : table == o.table;
    case Kind::Array:
      return (array && o.array) ? *array == *o.array : array == o.array;
  }
  return false;
}

}  // namespace protobus::amqp
