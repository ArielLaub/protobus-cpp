// The golden values were produced by the TypeScript reference implementation
// (protobus 2.4.0, MessageFactory), not derived by hand; protobus-go pins the
// same bytes. They keep the C++ encoder byte-for-byte with what a TypeScript
// peer emits, so captured traffic, DLQ contents and golden tests stay
// comparable across all four ports.
#include <gtest/gtest.h>

#include "wire/envelope.h"

namespace protobus::wire {
namespace {

std::string hex(const std::string& h) {
  std::string out;
  for (size_t i = 0; i < h.size(); i += 2) out.push_back(static_cast<char>(std::stoi(h.substr(i, 2), nullptr, 16)));
  return out;
}

std::string toHex(const std::string& b) {
  static const char* d = "0123456789abcdef";
  std::string out;
  for (unsigned char c : b) {
    out.push_back(d[c >> 4]);
    out.push_back(d[c & 15]);
  }
  return out;
}

TEST(Wire, RequestMatchesTypeScript) {
  EXPECT_EQ(toHex(encodeRequest({"T.Svc.add", "", hex("0801")})), "0a09542e5376632e6164641a020801");
  // proto3 would drop an all-default payload; TypeScript always writes the
  // data field, even empty.
  EXPECT_EQ(toHex(encodeRequest({"T.Svc.add", "x", ""})), "0a09542e5376632e6164641201781a00");
}

TEST(Wire, ResponseMatchesTypeScript) {
  Response result;
  result.result = Result{"T.Svc.add", hex("0802")};
  EXPECT_EQ(toHex(encodeResponse(result)), "0a0f0a09542e5376632e61646412020802");

  Response empty;
  empty.result = Result{"T.Svc.add", ""};
  EXPECT_EQ(toHex(encodeResponse(empty)), "0a0d0a09542e5376632e6164641200");

  // An empty code is written explicitly, as TypeScript does.
  Response noCode;
  noCode.error = ResponseError{"T.Svc.add", "boom", ""};
  EXPECT_EQ(toHex(encodeResponse(noCode)), "12130a09542e5376632e6164641204626f6f6d1a00");

  Response withCode;
  withCode.error = ResponseError{"T.Svc.add", "m", "C"};
  EXPECT_EQ(toHex(encodeResponse(withCode)), "12110a09542e5376632e61646412016d1a0143");
}

TEST(Wire, ResponseMustCarryExactlyOneMember) {
  EXPECT_THROW(encodeResponse(Response{}), std::invalid_argument);
  Response both;
  both.result = Result{"a", ""};
  both.error = ResponseError{"a", "", ""};
  EXPECT_THROW(encodeResponse(both), std::invalid_argument);
}

TEST(Wire, EventMatchesTypeScript) {
  EXPECT_EQ(toHex(encodeEvent({"T.Ev", "EVENT.T.Ev", hex("0a0179")})), "0a04542e4576120a4556454e542e542e45761a030a0179");
}

TEST(Wire, DecodesTypeScriptRequest) {
  // The actor written as an explicit empty string.
  auto r = decodeRequest(hex("0a09542e5376632e61646412001a020801"));
  EXPECT_EQ(r.method, "T.Svc.add");
  EXPECT_EQ(r.actor, "");
  EXPECT_EQ(r.data, hex("0801"));
}

TEST(Wire, DecodesTypeScriptResponses) {
  auto e = decodeResponse(hex("12110a09542e5376632e61646412016d1a0143"));
  ASSERT_TRUE(e.error);
  EXPECT_FALSE(e.result);
  EXPECT_EQ(e.error->method, "T.Svc.add");
  EXPECT_EQ(e.error->message, "m");
  EXPECT_EQ(e.error->code, "C");

  auto r = decodeResponse(hex("0a0d0a09542e5376632e6164641200"));
  ASSERT_TRUE(r.result);
  EXPECT_EQ(r.result->method, "T.Svc.add");
  EXPECT_TRUE(r.result->data.empty());
}

TEST(Wire, ErrorWinsOverResult) {
  // TypeScript checks `error` first; a container carrying both must read as
  // an error on every port, or the same bytes mean success in one language
  // and failure in another.
  std::string b;
  appendBytesField(b, 1, encodeResult({"a.B.c", ""}));
  std::string err;
  appendBytesField(err, 1, "a.B.c");
  appendBytesField(err, 2, "no");
  appendBytesField(b, 2, err);
  auto r = decodeResponse(b);
  ASSERT_TRUE(r.error);
  EXPECT_FALSE(r.result);
}

TEST(Wire, EmptyResponseIsRefused) { EXPECT_THROW(decodeResponse(""), MalformedError); }

TEST(Wire, DecodesTypeScriptEvent) {
  auto e = decodeEvent(hex("0a04542e4576120a4556454e542e542e45761a030a0179"));
  EXPECT_EQ(e.type, "T.Ev");
  EXPECT_EQ(e.topic, "EVENT.T.Ev");
  EXPECT_EQ(e.data, hex("0a0179"));
}

TEST(Wire, SkipsUnknownFields) {
  // A future peer adding a field must not break an older reader.
  std::string b = encodeRequest({"a.B.c", "", "\x01"});
  appendTag(b, 99, 0);
  appendVarint(b, 7);
  EXPECT_EQ(decodeRequest(b).method, "a.B.c");
}

TEST(Wire, RejectsMalformedInput) {
  for (const auto& b : {hex("0a0954"), hex("00"), hex("0801"), hex("0aff"), hex("0a01ff")}) {
    EXPECT_THROW(decodeRequest(b), MalformedError) << toHex(b);
  }
}

TEST(Wire, RoundTrips) {
  Request in{"pkg.sub.Service.method", "user:42", std::string(300, '\xab')};
  auto out = decodeRequest(encodeRequest(in));
  EXPECT_EQ(out.method, in.method);
  EXPECT_EQ(out.actor, in.actor);
  EXPECT_EQ(out.data, in.data);
}

}  // namespace
}  // namespace protobus::wire
