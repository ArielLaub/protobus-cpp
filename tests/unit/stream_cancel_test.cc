// A stream the caller's side fails (a buffer limit, a lost chunk) is a stream
// the producer must stop: exactly one cancellation notice goes out, the
// caller still sees the original error, and later chunks are ignored.
#include <gtest/gtest.h>

#include "helpers.h"

namespace {

using pbtesting::CalcService;
using pbtesting::eventually;
using pbtesting::MemoryBus;

class StreamCancelTest : public MemoryBus {
 protected:
  void SetUp() override {
    MemoryBus::SetUp();
    // Every cancellation notice also lands here, where it can be counted.
    auto ch = ctx->connection().openChannel();
    protobus::QueueOptions q;
    ctx->connection().declareExchange(ch, "proto.bus.cancel", "fanout");
    ctx->connection().declareQueue(ch, "notices", q);
    ctx->connection().bindQueue(ch, "notices", "proto.bus.cancel", "");
    ctx->connection().declareQueue(ch, "fake", q);
    ctx->connection().bindQueue(ch, "fake", "proto.bus", "REQUEST.fake.#");
    ctx->connection().closeChannel(ch);
  }

  size_t notices(const std::string& correlationId = "") {
    broker->flush();
    size_t n = 0;
    for (const auto& d : broker->peek("notices")) {
      if (correlationId.empty() || d.properties.correlationId == correlationId) ++n;
    }
    return n;
  }

  // A raw streaming call the test answers itself, chunk by chunk.
  struct RawCall {
    protobus::ChunkStream stream;
    std::string id;
    std::string replyTo;
  };

  RawCall rawStream() {
    RawCall c;
    c.stream = ctx->publishStreamingMessage("request", "REQUEST.fake.stream");
    EXPECT_TRUE(eventually([&] { return broker->queueDepth("fake") == 1; }));
    auto req = broker->peek("fake").at(0);
    c.id = req.properties.correlationId.value_or("");
    c.replyTo = req.properties.replyTo.value_or("");
    auto ch = ctx->connection().openChannel();
    ctx->connection().purgeQueue(ch, "fake");
    ctx->connection().closeChannel(ch);
    return c;
  }

  void sendChunk(const RawCall& c, const std::string& body, int64_t seq, bool final = false) {
    auto ch = ctx->connection().openChannel();
    protobus::PublishOptions p;
    p.properties.correlationId = c.id;
    protobus::amqp::FieldTable h;
    h[protobus::Config::HEADER_SEQ] = protobus::amqp::FieldValue::fromInt(seq);
    h[protobus::Config::HEADER_FINAL] = protobus::amqp::FieldValue::fromBool(final);
    p.properties.headers = h;
    ctx->connection().publish(ch, "proto.bus.callback", c.replyTo, body, p);
    ctx->connection().closeChannel(ch);
  }
};

class BufferLimit : public StreamCancelTest, public ::testing::WithParamInterface<const char*> {};

// Each bound in turn, on a producer that keeps going until told to stop.
TEST_P(BufferLimit, AnOverflowCancelsTheProducerOnce) {
  env.set(GetParam(), GetParam() == std::string("STREAM_MAX_BUFFERED_CHUNKS") ? "1" : "16");
  auto svc = serve();
  auto calc = proxy();
  pbtest::TickRequest r;
  r.set_count(400);
  r.set_delay_ms(5);
  {
    // Never iterated until it has already failed.
    auto stream = calc.ticks(r);
    ASSERT_TRUE(eventually([&] { return notices() == 1; })) << "no cancellation notice was published";
    ASSERT_TRUE(eventually([&] { return svc->stoppedEarly.load(); })) << "the producer never stopped";
    EXPECT_FALSE(svc->finished.load());
    EXPECT_THROW(stream.next(), protobus::StreamBackpressureError);
    // The error is the stream's answer from now on, and nothing more is sent.
    EXPECT_THROW(stream.next(), protobus::StreamBackpressureError);
    stream.cancel();
    stream.cancel();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(notices(), 1u);
}

INSTANTIATE_TEST_SUITE_P(Limits, BufferLimit,
                         ::testing::Values("STREAM_MAX_BUFFERED_CHUNKS", "STREAM_MAX_BUFFERED_BYTES",
                                           "STREAM_MAX_TOTAL_BUFFERED_BYTES"));

TEST_F(StreamCancelTest, ASequenceGapCancelsTheProducerBeforeTheCallerIterates) {
  auto call = rawStream();
  ASSERT_FALSE(call.id.empty());
  sendChunk(call, "zero", 0);
  sendChunk(call, "two", 2);
  ASSERT_TRUE(eventually([&] { return notices(call.id) == 1; }));
  EXPECT_THROW(call.stream.next(), protobus::StreamSequenceError);
  // The producer carries on for a moment; what it sends is ignored.
  sendChunk(call, "three", 3);
  sendChunk(call, "four", 4, true);
  EXPECT_THROW(call.stream.next(), protobus::StreamSequenceError);
  call.stream.cancel();
  { auto gone = std::move(call.stream); }
  EXPECT_EQ(notices(call.id), 1u);
}

TEST_F(StreamCancelTest, ASequenceGapAfterIterationStartedCancelsOnce) {
  auto call = rawStream();
  sendChunk(call, "zero", 0);
  EXPECT_EQ(call.stream.next(), std::optional<std::string>("zero"));
  sendChunk(call, "five", 5);
  EXPECT_THROW(call.stream.next(), protobus::StreamSequenceError);
  { auto gone = std::move(call.stream); }
  // Published from a worker: wait for it, then make sure no second follows.
  EXPECT_TRUE(eventually([&] { return notices(call.id) == 1; }));
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  EXPECT_EQ(notices(call.id), 1u);
}

TEST_F(StreamCancelTest, AnExplicitCancelStillSendsOneNotice) {
  auto call = rawStream();
  sendChunk(call, "zero", 0);
  call.stream.cancel();
  call.stream.cancel();
  { auto gone = std::move(call.stream); }
  EXPECT_TRUE(eventually([&] { return notices(call.id) == 1; }));
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  EXPECT_EQ(notices(call.id), 1u);
}

TEST_F(StreamCancelTest, ACompletedStreamSendsNoNotice) {
  auto call = rawStream();
  sendChunk(call, "zero", 0);
  sendChunk(call, "one", 1, true);
  EXPECT_EQ(call.stream.next(), std::optional<std::string>("zero"));
  EXPECT_EQ(call.stream.next(), std::optional<std::string>("one"));
  EXPECT_FALSE(call.stream.next());
  { auto gone = std::move(call.stream); }
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  EXPECT_EQ(notices(call.id), 0u);
}

}  // namespace
