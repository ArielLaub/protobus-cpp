#include <gtest/gtest.h>

#include "helpers.h"
#include "wire/envelope.h"

namespace {

using pbtesting::CalcService;
using pbtesting::eventually;
using pbtesting::MemoryBus;

using StreamTest = MemoryBus;

pbtest::TickRequest ticks(int count) {
  pbtest::TickRequest r;
  r.set_count(count);
  return r;
}

TEST_F(StreamTest, ChunksArriveInOrder) {
  serve();
  auto calc = proxy();
  std::vector<int> seqs;
  for (const auto& t : calc.ticks(ticks(50))) {
    EXPECT_EQ(t.payload(), "chunk-" + std::to_string(t.seq()));
    seqs.push_back(t.seq());
  }
  ASSERT_EQ(seqs.size(), 50u);
  for (int i = 0; i < 50; ++i) EXPECT_EQ(seqs[i], i);
}

TEST_F(StreamTest, AnEmptyStreamEnds) {
  serve();
  auto calc = proxy();
  auto stream = calc.ticks(ticks(0));
  EXPECT_FALSE(stream.next());
}

TEST_F(StreamTest, AHandledErrorEndsTheStreamAfterWhatWasSent) {
  serve();
  auto calc = proxy();
  auto r = ticks(10);
  r.set_fail_at(2);
  int n = 0;
  try {
    for (const auto& t : calc.ticks(r)) {
      (void)t;
      ++n;
    }
    FAIL();
  } catch (const protobus::RemoteError& e) {
    EXPECT_EQ(e.code(), "TEST_FAIL");
    EXPECT_NE(std::string(e.what()).find("deliberate failure at chunk 2"), std::string::npos);
  }
  EXPECT_EQ(n, 2);
}

TEST_F(StreamTest, AnUnhandledErrorEndsTheStreamWithoutARetry) {
  auto svc = serve();
  auto calc = proxy();
  auto r = ticks(10);
  r.set_fail_at(1);
  r.set_unhandled(true);
  try {
    for (const auto& t : calc.ticks(r)) (void)t;
    FAIL();
  } catch (const protobus::RemoteError& e) {
    EXPECT_STREQ(e.what(), "stream broke");
  }
  EXPECT_EQ(broker->queueDepth("pbtest.Calc.Retry"), 0u);
}

TEST_F(StreamTest, LeavingTheLoopCancelsTheProducer) {
  auto svc = serve();
  auto calc = proxy();
  auto r = ticks(500);
  r.set_delay_ms(10);
  int n = 0;
  {
    auto stream = calc.ticks(r);
    for (const auto& t : stream) {
      (void)t;
      if (++n == 3) break;
    }
  }  // the stream is destroyed here, unfinished: that cancels it
  ASSERT_TRUE(eventually([&] { return svc->stoppedEarly.load(); }));
  EXPECT_FALSE(svc->finished.load());
  EXPECT_LT(svc->yielded.load(), 500);
}

TEST_F(StreamTest, AnAbortSignalCancelsFromAnotherThread) {
  auto svc = serve();
  auto calc = proxy();
  auto r = ticks(500);
  r.set_delay_ms(10);
  protobus::AbortController controller;
  protobus::StreamOptions o;
  o.signal = controller.signal();
  auto stream = calc.ticks(r, o);
  ASSERT_TRUE(stream.next());
  std::thread([&] { controller.abort(); }).join();
  // A cancelled stream ends rather than raising.
  int more = 0;
  while (stream.next()) ++more;
  EXPECT_LT(more, 500);
  ASSERT_TRUE(eventually([&] { return svc->stoppedEarly.load(); }));
}

TEST_F(StreamTest, AnAbortedSignalSendsNothing) {
  auto svc = serve();
  auto calc = proxy();
  protobus::AbortController controller;
  controller.abort();
  protobus::StreamOptions o;
  o.signal = controller.signal();
  auto stream = calc.ticks(ticks(5), o);
  EXPECT_FALSE(stream.next());
  broker->flush();
  EXPECT_EQ(svc->yielded.load(), 0);
}

TEST_F(StreamTest, AStalledStreamTimesOutAndCancelsTheProducer) {
  auto svc = serve();
  auto calc = proxy();
  auto r = ticks(3);
  r.set_delay_ms(5000);
  protobus::StreamOptions o;
  o.idleTimeoutMs = 100;
  auto stream = calc.ticks(r, o);
  // Chunks go out with a look-ahead of one (so the last can carry the final
  // flag), so a producer this slow has published nothing yet.
  EXPECT_THROW(stream.next(), protobus::StreamTimeoutError);
  // The producer is told to stop rather than run on for nobody.
  ASSERT_TRUE(eventually([&] { return svc->stoppedEarly.load(); }));
}

TEST_F(StreamTest, AConsumerThatFallsBehindFailsWithBackpressure) {
  env.set("STREAM_MAX_BUFFERED_CHUNKS", "3");
  serve();
  auto calc = proxy();
  auto stream = calc.ticks(ticks(50));
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  try {
    while (stream.next()) {
    }
    FAIL() << "expected backpressure";
  } catch (const protobus::StreamBackpressureError& e) {
    EXPECT_NE(std::string(e.what()).find("the consumer is not keeping up"), std::string::npos);
  }
}

// A server that replies by hand, so the reply headers can misbehave.
class RawReplier {
 public:
  RawReplier(protobus::Context& ctx, std::vector<std::pair<int, bool>> frames) {
    channel_ = ctx.connection().openChannel();
    ctx.connection().declareExchange(channel_, "proto.bus", "topic");
    protobus::QueueOptions q;
    ctx.connection().declareQueue(channel_, "pbtest.Calc", q);
    ctx.connection().bindQueue(channel_, "pbtest.Calc", "proto.bus", "REQUEST.pbtest.Calc.*");
    auto conn = ctx.connectionPtr();
    auto ch = channel_;
    channel_->consume("pbtest.Calc", "raw", true, false,
                      [conn, ch, frames](protobus::amqp::Delivery d) {
                        std::thread([conn, ch, frames, d] {
                          for (const auto& [seq, final] : frames) {
                            pbtest::Tick t;
                            t.set_seq(seq);
                            protobus::PublishOptions p;
                            p.properties.correlationId = d.properties.correlationId;
                            protobus::amqp::FieldTable h;
                            h["x-protobus-seq"] = protobus::amqp::FieldValue::fromInt(seq);
                            h["x-protobus-final"] = protobus::amqp::FieldValue::fromBool(final);
                            p.properties.headers = h;
                            conn->publish(ch, "proto.bus.callback", *d.properties.replyTo,
                                          protobus::MessageFactory::buildResultResponse("pbtest.Calc.ticks",
                                                                                        t.SerializeAsString()),
                                          p);
                          }
                        }).detach();
                      },
                      nullptr);
  }

 private:
  std::shared_ptr<protobus::amqp::Channel> channel_;
};

TEST_F(StreamTest, ALostChunkFailsTheStreamRatherThanTruncatingIt) {
  RawReplier server(*ctx, {{0, false}, {2, true}});
  auto calc = proxy();
  auto stream = calc.ticks(ticks(3));
  ASSERT_TRUE(stream.next());
  EXPECT_THROW(stream.next(), protobus::StreamSequenceError);
}

TEST_F(StreamTest, ADuplicateChunkIsDropped) {
  RawReplier server(*ctx, {{0, false}, {0, false}, {1, true}});
  auto calc = proxy();
  std::vector<int> seqs;
  for (const auto& t : calc.ticks(ticks(2))) seqs.push_back(t.seq());
  EXPECT_EQ(seqs, (std::vector<int>{0, 1}));
}

TEST_F(StreamTest, StreamsInterleaveWithoutMixing) {
  serve<CalcService>({}, nullptr);
  auto calc = proxy();
  auto a = calc.ticks(ticks(20));
  auto b = calc.ticks(ticks(20));
  for (int i = 0; i < 20; ++i) {
    EXPECT_EQ(a.next()->seq(), i);
    EXPECT_EQ(b.next()->seq(), i);
  }
  EXPECT_FALSE(a.next());
  EXPECT_FALSE(b.next());
}

}  // namespace
