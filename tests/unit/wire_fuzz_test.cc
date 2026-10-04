// Property tests for the envelope decoders, which read bytes any publisher can
// write: arbitrary and mutated input must be refused cleanly or decoded, and
// whatever decodes must survive a round trip unchanged.
#include <gtest/gtest.h>

#include <random>

#include "wire/envelope.h"

namespace protobus::wire {
namespace {

std::string mutate(std::string b, std::mt19937& gen) {
  std::uniform_int_distribution<int> op(0, 3);
  std::uniform_int_distribution<int> byte(0, 255);
  const int edits = 1 + static_cast<int>(gen() % 4);
  for (int i = 0; i < edits; ++i) {
    switch (op(gen)) {
      case 0:  // flip a byte
        if (!b.empty()) b[gen() % b.size()] = static_cast<char>(byte(gen));
        break;
      case 1:  // insert
        b.insert(b.begin() + static_cast<long>(b.empty() ? 0 : gen() % b.size()), static_cast<char>(byte(gen)));
        break;
      case 2:  // truncate
        if (!b.empty()) b.resize(gen() % b.size());
        break;
      case 3:  // append garbage
        b.push_back(static_cast<char>(byte(gen)));
        break;
    }
  }
  return b;
}

std::string randomBytes(std::mt19937& gen) {
  std::string b(gen() % 64, '\0');
  for (auto& c : b) c = static_cast<char>(gen() & 0xff);
  return b;
}

TEST(WireFuzz, RequestsDecodeOrAreRefusedAndRoundTrip) {
  std::mt19937 gen(12345);
  const std::vector<std::string> seeds = {
      encodeRequest({"a.B.c", "actor", "\x08\x01"}),
      encodeRequest({"pkg.sub.Service.method", "", std::string(300, '\xab')}),
  };
  int decoded = 0;
  for (int i = 0; i < 50000; ++i) {
    const std::string input = i % 2 ? randomBytes(gen) : mutate(seeds[gen() % seeds.size()], gen);
    Request r;
    try {
      r = decodeRequest(input);
    } catch (const MalformedError&) {
      continue;
    }
    ++decoded;
    const Request again = decodeRequest(encodeRequest(r));
    ASSERT_EQ(again.method, r.method);
    ASSERT_EQ(again.actor, r.actor);
    ASSERT_EQ(again.data, r.data);
  }
  EXPECT_GT(decoded, 0);
}

TEST(WireFuzz, ResponsesDecodeOrAreRefused) {
  std::mt19937 gen(54321);
  Response ok;
  ok.result = Result{"a.B.c", "\x08\x02"};
  Response err;
  err.error = ResponseError{"a.B.c", "boom", "CODE"};
  const std::vector<std::string> seeds = {encodeResponse(ok), encodeResponse(err)};
  for (int i = 0; i < 50000; ++i) {
    const std::string input = i % 2 ? randomBytes(gen) : mutate(seeds[gen() % seeds.size()], gen);
    try {
      Response r = decodeResponse(input);
      ASSERT_NE(r.result.has_value(), r.error.has_value());
      const Response again = decodeResponse(encodeResponse(r));
      ASSERT_EQ(again.error.has_value(), r.error.has_value());
    } catch (const MalformedError&) {
    }
  }
}

TEST(WireFuzz, EventsDecodeOrAreRefused) {
  std::mt19937 gen(999);
  const std::string seed = encodeEvent({"a.B", "EVENT.a.B", "\x0a\x01\x79"});
  for (int i = 0; i < 50000; ++i) {
    const std::string input = i % 2 ? randomBytes(gen) : mutate(seed, gen);
    try {
      Event e = decodeEvent(input);
      const Event again = decodeEvent(encodeEvent(e));
      ASSERT_EQ(again.type, e.type);
      ASSERT_EQ(again.topic, e.topic);
      ASSERT_EQ(again.data, e.data);
    } catch (const MalformedError&) {
    }
  }
}

TEST(WireFuzz, DeeplyNestedGroupsAreRefusedWithoutExhaustingTheStack) {
  // An unknown field opening thousands of nested groups.
  std::string b;
  for (int i = 0; i < 100000; ++i) appendTag(b, 9, 3);
  EXPECT_THROW(decodeRequest(b), MalformedError);
}

}  // namespace
}  // namespace protobus::wire
