// The outstanding-confirm bound counts what the transport still holds, not
// what callers are still waiting for: a publish that timed out ambiguously
// keeps its slot until the broker resolves it, a channel whose slots are all
// stuck that way is retired, and work parked behind the bound is itself
// bounded.
#include <gtest/gtest.h>

#include <map>

#include "helpers.h"

namespace {

using pbtesting::eventually;
using pbtesting::MemoryBus;
using protobus::testing::MemoryBroker;

class ConfirmBoundTest : public MemoryBus {
 protected:
  std::shared_ptr<protobus::amqp::Channel> channelWithQueue(const std::string& queue) {
    auto ch = ctx->connection().openChannel();
    ctx->connection().declareExchange(ch, "test.x", "direct");
    protobus::QueueOptions q;
    ctx->connection().declareQueue(ch, queue, q);
    ctx->connection().bindQueue(ch, queue, "test.x", queue);
    return ch;
  }

  // What each async publish completed with, by index, counting completions.
  struct Outcomes {
    std::mutex m;
    std::map<int, std::vector<std::string>> by;
    void record(int i, std::exception_ptr e) {
      std::string what = "ok";
      if (e) {
        try {
          std::rethrow_exception(e);
        } catch (const protobus::PublishError& err) {
          what = std::string(err.name()) + (err.ambiguous() ? "(ambiguous)" : "(definite)");
        } catch (const std::exception& err) {
          what = std::string("other: ") + err.what();
        }
      }
      std::lock_guard<std::mutex> lock(m);
      by[i].push_back(what);
    }
    size_t completed() {
      std::lock_guard<std::mutex> lock(m);
      return by.size();
    }
    int count(const std::string& what) {
      std::lock_guard<std::mutex> lock(m);
      int n = 0;
      for (const auto& [_, v] : by) n += v.at(0) == what ? 1 : 0;
      return n;
    }
    bool eachOnce() {
      std::lock_guard<std::mutex> lock(m);
      for (const auto& [_, v] : by) {
        if (v.size() != 1) return false;
      }
      return true;
    }
  };

  void publishMany(const std::shared_ptr<protobus::amqp::Channel>& ch, int n, Outcomes& out, int base = 0) {
    for (int i = 0; i < n; ++i) {
      ctx->connection().publishAsync(ch, "test.x", "q1", "x", {},
                                     [&out, i, base](std::exception_ptr e) { out.record(base + i, e); });
    }
  }
};

// The reviewer's reproduction: with every confirm dropped, timeouts must not
// let more than the bound reach the transport.
TEST_F(ConfirmBoundTest, TimedOutPublishesKeepTheirSlots) {
  env.set("MAX_OUTSTANDING_CONFIRMS", "2");
  env.set("PUBLISH_CONFIRM_TIMEOUT_MS", "50");
  auto ch = channelWithQueue("q1");
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Drop);
  Outcomes out;
  size_t maxHeld = 0;
  for (int round = 0; round < 5; ++round) {
    publishMany(ch, 2, out, round * 2);
    for (int i = 0; i < 20; ++i) {
      maxHeld = std::max(maxHeld, broker->heldConfirms());
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  }
  ASSERT_TRUE(eventually([&] { return out.completed() == 10; }));
  EXPECT_LE(maxHeld, 2u);
  // Only the first two ever reached the broker.
  EXPECT_LE(broker->queueDepth("q1"), 2u);
  EXPECT_TRUE(out.eachOnce());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_TRUE(out.eachOnce());
}

// A channel whose every slot is held by an ambiguous timeout can make no
// progress: it is retired, which resolves what it held, and a dispatcher
// publishing on it moves to a new channel.
TEST_F(ConfirmBoundTest, AStuckChannelIsRetiredAndPublishingRecovers) {
  env.set("MAX_OUTSTANDING_CONFIRMS", "2");
  env.set("PUBLISH_CONFIRM_TIMEOUT_MS", "50");
  auto ch = channelWithQueue("q1");
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Drop);
  Outcomes out;
  publishMany(ch, 2, out);
  ASSERT_TRUE(eventually([&] { return out.completed() == 2; }));
  EXPECT_EQ(out.count("PublishConfirmTimeoutError(ambiguous)"), 2);
  ASSERT_TRUE(eventually([&] { return !ch->isOpen(); })) << "the stuck channel was not retired";
  EXPECT_EQ(broker->heldConfirms(), 0u);

  // Events go through the dispatcher's own channel: stuck, retired, replaced.
  pbtest::Ping p;
  p.set_id("e");
  for (int i = 0; i < 2; ++i) EXPECT_THROW(ctx->publishEvent(p), protobus::PublishConfirmTimeoutError);
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Ack);
  EXPECT_TRUE(eventually([&] {
    try {
      ctx->publishEvent(p);
      return true;
    } catch (const std::exception&) {
      return false;
    }
  }));
}

// A late confirm frees the slot it held, and the caller hears nothing more.
TEST_F(ConfirmBoundTest, ALateConfirmFreesItsSlot) {
  env.set("MAX_OUTSTANDING_CONFIRMS", "2");
  env.set("PUBLISH_CONFIRM_TIMEOUT_MS", "40");
  auto ch = channelWithQueue("q1");
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Drop);
  Outcomes out;
  publishMany(ch, 1, out);
  ASSERT_TRUE(eventually([&] { return out.completed() == 1; }));
  EXPECT_EQ(broker->heldConfirms(), 1u);
  EXPECT_TRUE(ch->isOpen());
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Ack);
  EXPECT_EQ(broker->releaseHeldConfirms(), 1u);
  broker->flush();
  publishMany(ch, 2, out, 1);
  ASSERT_TRUE(eventually([&] { return out.completed() == 3; }));
  EXPECT_EQ(out.count("ok"), 2);
  EXPECT_EQ(broker->queueDepth("q1"), 3u);
  EXPECT_TRUE(out.eachOnce());
}

// Parked publishes are bounded too, and one that cannot be parked fails at
// once, definitely: it was never sent.
TEST_F(ConfirmBoundTest, ParkedWorkIsBounded) {
  env.set("MAX_OUTSTANDING_CONFIRMS", "1");
  env.set("MAX_PARKED_PUBLISHES", "3");
  env.set("PUBLISH_CONFIRM_TIMEOUT_MS", "5000");
  auto ch = channelWithQueue("q1");
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Drop);
  Outcomes out;
  publishMany(ch, 10, out);
  ASSERT_TRUE(eventually([&] { return out.completed() == 6; }));
  EXPECT_EQ(out.count("PublishBacklogError(definite)"), 6);
  EXPECT_EQ(broker->queueDepth("q1"), 1u);
  // The parked three go out, one at a time, as confirms arrive.
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Ack);
  broker->releaseHeldConfirms();
  ASSERT_TRUE(eventually([&] { return out.completed() == 10; }));
  EXPECT_EQ(out.count("ok"), 4);
  EXPECT_EQ(broker->queueDepth("q1"), 4u);
  EXPECT_TRUE(out.eachOnce());
}

// A publish parked behind the bound still answers within its deadline.
TEST_F(ConfirmBoundTest, AParkedPublishTimesOutDefinitely) {
  env.set("MAX_OUTSTANDING_CONFIRMS", "1");
  env.set("PUBLISH_CONFIRM_TIMEOUT_MS", "60");
  auto ch = channelWithQueue("q1");
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Drop);
  Outcomes out;
  publishMany(ch, 3, out);
  ASSERT_TRUE(eventually([&] { return out.completed() == 3; }));
  // The one that went out is ambiguous; the parked ones were never sent.
  EXPECT_EQ(out.count("PublishConfirmTimeoutError(ambiguous)"), 1);
  EXPECT_EQ(out.count("PublishBacklogError(definite)") + out.count("ChannelClosedError(ambiguous)"), 2);
  EXPECT_EQ(broker->queueDepth("q1"), 1u);
  EXPECT_TRUE(out.eachOnce());
}

// A service's consumer channel is also its reply channel. When its replies'
// confirms stop, the channel is retired and the listener rebuilt, and the
// service answers again once confirms resume.
TEST_F(ConfirmBoundTest, AConsumerChannelRetiredForStuckConfirmsIsRebuilt) {
  env.set("MAX_OUTSTANDING_CONFIRMS", "1");
  env.set("PUBLISH_CONFIRM_TIMEOUT_MS", "50");
  serve();
  auto calc = proxy();
  pbtest::AddRequest r;
  r.set_a(1);
  r.set_b(2);
  EXPECT_EQ(calc.add(r).result(), 3);
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Drop);
  protobus::CallOptions o;
  o.timeoutMs = 500;
  try {
    calc.add(r, o);
  } catch (const std::exception&) {
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Ack);
  broker->releaseHeldConfirms();
  EXPECT_TRUE(eventually([&] {
    try {
      protobus::CallOptions quick;
      quick.timeoutMs = 300;
      return calc.add(r, quick).result() == 3;
    } catch (const std::exception&) {
      return false;
    }
  }));
  EXPECT_TRUE(eventually([&] { return broker->consumerCount("pbtest.Calc") == 1; }));
}

}  // namespace
