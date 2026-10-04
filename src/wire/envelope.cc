#include "wire/envelope.h"

#include <cstdint>
#include <functional>
#include <stdexcept>

namespace protobus::wire {

namespace {

constexpr uint32_t kVarint = 0;
constexpr uint32_t kFixed64 = 1;
constexpr uint32_t kBytes = 2;
constexpr uint32_t kStartGroup = 3;
constexpr uint32_t kEndGroup = 4;
constexpr uint32_t kFixed32 = 5;

class Reader {
 public:
  explicit Reader(std::string_view b) : b_(b) {}

  bool done() const { return pos_ >= b_.size(); }

  uint64_t varint() {
    uint64_t v = 0;
    for (int shift = 0; shift < 70; shift += 7) {
      if (pos_ >= b_.size()) throw MalformedError("truncated varint");
      const uint8_t c = static_cast<uint8_t>(b_[pos_++]);
      if (shift == 63 && c > 1) throw MalformedError("varint overflows 64 bits");
      v |= static_cast<uint64_t>(c & 0x7f) << shift;
      if ((c & 0x80) == 0) return v;
    }
    throw MalformedError("varint too long");
  }

  std::string_view bytes() {
    const uint64_t n = varint();
    if (n > b_.size() - pos_) throw MalformedError("length exceeds the buffer");
    std::string_view v = b_.substr(pos_, n);
    pos_ += n;
    return v;
  }

  void skip(uint32_t field, uint32_t wireType, int depth = 0) {
    switch (wireType) {
      case kVarint:
        varint();
        return;
      case kFixed64:
        advance(8);
        return;
      case kBytes:
        bytes();
        return;
      case kFixed32:
        advance(4);
        return;
      case kStartGroup: {
        if (depth > 64) throw MalformedError("groups nested too deeply");
        for (;;) {
          if (done()) throw MalformedError("unterminated group");
          const uint64_t tag = varint();
          const uint32_t f = static_cast<uint32_t>(tag >> 3);
          const uint32_t t = static_cast<uint32_t>(tag & 7);
          if (f == 0) throw MalformedError("field number 0");
          if (t == kEndGroup) {
            if (f != field) throw MalformedError("mismatched end group");
            return;
          }
          skip(f, t, depth + 1);
        }
      }
      default:
        throw MalformedError("invalid wire type " + std::to_string(wireType));
    }
  }

 private:
  void advance(size_t n) {
    if (n > b_.size() - pos_) throw MalformedError("truncated fixed-width field");
    pos_ += n;
  }

  std::string_view b_;
  size_t pos_ = 0;
};

bool validUtf8(std::string_view s) {
  size_t i = 0;
  while (i < s.size()) {
    const auto c = static_cast<uint8_t>(s[i]);
    size_t n;
    uint32_t cp;
    if (c < 0x80) {
      ++i;
      continue;
    } else if ((c & 0xe0) == 0xc0) {
      n = 1;
      cp = c & 0x1f;
    } else if ((c & 0xf0) == 0xe0) {
      n = 2;
      cp = c & 0x0f;
    } else if ((c & 0xf8) == 0xf0) {
      n = 3;
      cp = c & 0x07;
    } else {
      return false;
    }
    // The lead byte at i needs n continuation bytes after it.
    if (s.size() - i <= n) return false;
    for (size_t k = 1; k <= n; ++k) {
      const auto cc = static_cast<uint8_t>(s[i + k]);
      if ((cc & 0xc0) != 0x80) return false;
      cp = (cp << 6) | (cc & 0x3f);
    }
    // Overlong forms, surrogates and values beyond U+10FFFF.
    if ((n == 1 && cp < 0x80) || (n == 2 && cp < 0x800) || (n == 3 && cp < 0x10000)) return false;
    if (cp >= 0xd800 && cp <= 0xdfff) return false;
    if (cp > 0x10ffff) return false;
    i += n + 1;
  }
  return true;
}

std::string str(uint32_t field, std::string_view v) {
  if (!validUtf8(v)) throw MalformedError("field " + std::to_string(field) + " is not valid UTF-8");
  return std::string(v);
}

using FieldFn = std::function<void(uint32_t field, std::string_view value)>;

// Visits every field of a message, skipping unknown ones so that a newer peer
// adding a field cannot break an older reader. Envelopes contain only
// length-delimited fields, so any other wire type on a known field is
// malformed.
void walk(std::string_view b, uint32_t maxKnown, const FieldFn& fn) {
  Reader r(b);
  while (!r.done()) {
    const uint64_t tag = r.varint();
    const uint32_t field = static_cast<uint32_t>(tag >> 3);
    const uint32_t wireType = static_cast<uint32_t>(tag & 7);
    if (field == 0 || tag >> 3 > 0x1fffffff) throw MalformedError("invalid field number");
    if (field > maxKnown) {
      r.skip(field, wireType);
      continue;
    }
    if (wireType != kBytes) {
      throw MalformedError("field " + std::to_string(field) + " has wire type " + std::to_string(wireType) +
                           ", want length-delimited");
    }
    fn(field, r.bytes());
  }
}

void decodeResultInto(std::string_view b, Result& r) {
  walk(b, 2, [&](uint32_t field, std::string_view v) {
    if (field == 1) {
      r.method = str(field, v);
    } else {
      r.data.assign(v);
    }
  });
}

void decodeErrorInto(std::string_view b, ResponseError& e) {
  walk(b, 3, [&](uint32_t field, std::string_view v) {
    switch (field) {
      case 1:
        e.method = str(field, v);
        break;
      case 2:
        e.message = str(field, v);
        break;
      case 3:
        e.code = str(field, v);
        break;
    }
  });
}

}  // namespace

void appendVarint(std::string& out, uint64_t v) {
  while (v >= 0x80) {
    out.push_back(static_cast<char>((v & 0x7f) | 0x80));
    v >>= 7;
  }
  out.push_back(static_cast<char>(v));
}

void appendTag(std::string& out, uint32_t field, uint32_t wireType) {
  appendVarint(out, (static_cast<uint64_t>(field) << 3) | wireType);
}

void appendBytesField(std::string& out, uint32_t field, std::string_view v) {
  appendTag(out, field, kBytes);
  appendVarint(out, v.size());
  out.append(v);
}

std::string encodeRequest(const Request& r) {
  std::string out;
  out.reserve(r.method.size() + r.actor.size() + r.data.size() + 16);
  appendBytesField(out, 1, r.method);
  if (!r.actor.empty()) appendBytesField(out, 2, r.actor);
  appendBytesField(out, 3, r.data);
  return out;
}

std::string encodeResult(const Result& r) {
  std::string out;
  out.reserve(r.method.size() + r.data.size() + 12);
  appendBytesField(out, 1, r.method);
  appendBytesField(out, 2, r.data);
  return out;
}

namespace {
std::string encodeError(const ResponseError& e) {
  std::string out;
  appendBytesField(out, 1, e.method);
  appendBytesField(out, 2, e.message);
  appendBytesField(out, 3, e.code);
  return out;
}
}  // namespace

std::string encodeResponse(const Response& r) {
  if (r.result.has_value() == r.error.has_value()) {
    throw std::invalid_argument("protobus: a response carries exactly one of a result and an error");
  }
  std::string out;
  if (r.result) {
    appendBytesField(out, 1, encodeResult(*r.result));
  } else {
    appendBytesField(out, 2, encodeError(*r.error));
  }
  return out;
}

std::string encodeEvent(const Event& e) {
  std::string out;
  out.reserve(e.type.size() + e.topic.size() + e.data.size() + 16);
  appendBytesField(out, 1, e.type);
  appendBytesField(out, 2, e.topic);
  appendBytesField(out, 3, e.data);
  return out;
}

Request decodeRequest(std::string_view b) {
  Request r;
  walk(b, 3, [&](uint32_t field, std::string_view v) {
    switch (field) {
      case 1:
        r.method = str(field, v);
        break;
      case 2:
        r.actor = str(field, v);
        break;
      case 3:
        r.data.assign(v);
        break;
    }
  });
  return r;
}

Response decodeResponse(std::string_view b) {
  Result result;
  ResponseError error;
  bool hasResult = false;
  bool hasError = false;
  walk(b, 2, [&](uint32_t field, std::string_view v) {
    // Repeated occurrences of an embedded message merge, as protobuf
    // requires; decoding into the same value does exactly that.
    if (field == 1) {
      hasResult = true;
      decodeResultInto(v, result);
    } else {
      hasError = true;
      decodeErrorInto(v, error);
    }
  });
  Response r;
  if (hasError) {
    r.error = std::move(error);
  } else if (hasResult) {
    r.result = std::move(result);
  } else {
    throw MalformedError("response carries neither a result nor an error");
  }
  return r;
}

Event decodeEvent(std::string_view b) {
  Event e;
  walk(b, 3, [&](uint32_t field, std::string_view v) {
    switch (field) {
      case 1:
        e.type = str(field, v);
        break;
      case 2:
        e.topic = str(field, v);
        break;
      case 3:
        e.data.assign(v);
        break;
    }
  });
  return e;
}

}  // namespace protobus::wire
