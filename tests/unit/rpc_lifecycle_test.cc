// The caller's side of a unary call across its whole life: the deadline
// against the broker confirm, closing with calls and streams pending, and
// pending state racing a disconnect.
#include <gtest/gtest.h>

#include <future>
#include <random>

#include "helpers.h"
#include "scheduler.h"

namespace {

using pbtesting::eventually;
using pbtesting::MemoryBus;
using protobus::testing::MemoryBroker;
using Clock = std::chrono::steady_clock;

int64_t msSince(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count();
}

class RpcLifecycleTest : public MemoryBus {
 protected:
  // A queue that takes requests for REQUEST.sink.* and never answers them.
  void bindSink() {
    auto ch = ctx->connection().openChannel();
    protobus::QueueOptions q;
    ctx->connection().declareQueue(ch, "sink", q);
    ctx->connection().bindQueue(ch, "sink", "proto.bus", "REQUEST.sink.#");
    ctx->connection().closeChannel(ch);
  }

  // Holds slow() until released, so a call stays pending.
  class HeldCalc : public pbtesting::CalcService {
   public:
    using CalcService::CalcService;
    std::mutex m;
    std::condition_variable cv;
    bool release = false;
    std::atomic<int> entered{0};
    pbtest::Nothing slow(const pbtest::SlowRequest&, protobus::CallContext&) override {
      ++entered;
      std::unique_lock<std::mutex> lock(m);
      cv.wait(lock, [&] { return release; });
      return {};
    }
    void open() {
      {
        std::lock_guard<std::mutex> lock(m);
        release = true;
      }
      cv.notify_all();
    }
  };
};

// ---- the deadline bounds the confirm wait ------------------------------------------

TEST_F(RpcLifecycleTest, TheDeadlineIsNotExtendedByAMissingConfirm) {
  env.set("PUBLISH_CONFIRM_TIMEOUT_MS", "2000");
  bindSink();
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Drop);
  protobus::CallOptions o;
  o.timeoutMs = 50;
  const auto start = Clock::now();
  try {
    ctx->publishMessage("x", "REQUEST.sink.call", o);
    FAIL() << "expected a timeout";
  } catch (const protobus::RpcTimeoutError&) {
    // The confirm never came, so the request may have been delivered: a
    // timeout, never a definite publish failure.
  }
  EXPECT_LT(msSince(start), 1000);
  broker->releaseHeldConfirms();
}

TEST_F(RpcLifecycleTest, AReplyBeforeTheConfirmCompletesTheCall) {
  env.set("PUBLISH_CONFIRM_TIMEOUT_MS", "3000");
  serve();
  auto calc = proxy();
  // Messages are still routed in Drop mode; only their confirms are held.
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Drop);
  pbtest::AddRequest r;
  r.set_a(2);
  r.set_b(3);
  protobus::CallOptions o;
  o.timeoutMs = 2000;
  const auto start = Clock::now();
  EXPECT_EQ(calc.add(r, o).result(), 5);
  EXPECT_LT(msSince(start), 1000);
  // The confirms arriving now are late outcomes for a call already done.
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Ack);
  broker->releaseHeldConfirms(protobus::amqp::ConfirmOutcome::Nack);
  EXPECT_EQ(calc.add(r).result(), 5);
}

TEST_F(RpcLifecycleTest, AConfirmBeforeTheReplyKeepsWaitingForTheReply) {
  serve();
  auto calc = proxy();
  pbtest::SlowRequest s;
  s.set_ms(100);
  protobus::CallOptions o;
  o.timeoutMs = 3000;
  const auto start = Clock::now();
  EXPECT_NO_THROW(calc.slow(s, o));
  EXPECT_GE(msSince(start), 90);
}

TEST_F(RpcLifecycleTest, LateConfirmsAfterATimeoutAreIgnored) {
  bindSink();
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Drop);
  for (auto outcome : {protobus::amqp::ConfirmOutcome::Ack, protobus::amqp::ConfirmOutcome::Nack,
                       protobus::amqp::ConfirmOutcome::Returned}) {
    protobus::CallOptions o;
    o.timeoutMs = 30;
    EXPECT_THROW(ctx->publishMessage("x", "REQUEST.sink.call", o), protobus::RpcTimeoutError);
    ASSERT_TRUE(eventually([&] { return broker->heldConfirms() == 1; }));
    EXPECT_EQ(broker->releaseHeldConfirms(outcome), 1u);
    broker->flush();
  }
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Ack);
  serve();
  auto calc = proxy();
  pbtest::AddRequest r;
  r.set_a(1);
  r.set_b(1);
  EXPECT_EQ(calc.add(r).result(), 2);
}

TEST_F(RpcLifecycleTest, ADefiniteFailureBeforeTheDeadlineIsReported) {
  protobus::CallOptions o;
  o.timeoutMs = 2000;
  const auto start = Clock::now();
  EXPECT_THROW(ctx->publishMessage("x", "REQUEST.nobody.home", o), protobus::UnroutableError);
  EXPECT_LT(msSince(start), 1000);
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Nack);
  EXPECT_THROW(ctx->publishMessage("x", "REQUEST.nobody.home", o), protobus::PublishNackedError);
}

TEST_F(RpcLifecycleTest, ADisconnectDuringPublicationFailsTheCallPromptly) {
  env.set("PUBLISH_CONFIRM_TIMEOUT_MS", "5000");
  bindSink();
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Drop);
  auto result = std::async(std::launch::async, [&] {
    protobus::CallOptions o;
    o.timeoutMs = 5000;
    const auto start = Clock::now();
    try {
      ctx->publishMessage("x", "REQUEST.sink.call", o);
      return std::make_pair(std::string("returned"), msSince(start));
    } catch (const protobus::DisconnectedError&) {
      return std::make_pair(std::string("disconnected"), msSince(start));
    } catch (const protobus::ChannelClosedError&) {
      return std::make_pair(std::string("channel-closed"), msSince(start));
    } catch (const std::exception& e) {
      return std::make_pair(std::string("other: ") + e.what(), msSince(start));
    }
  });
  ASSERT_TRUE(eventually([&] { return broker->heldConfirms() == 1; }));
  broker->killConnections();
  auto [outcome, elapsed] = result.get();
  // Either is honest: both say the request may have been delivered.
  EXPECT_TRUE(outcome == "disconnected" || outcome == "channel-closed") << outcome;
  EXPECT_LT(elapsed, 2000);
}

// ---- closing with work pending -------------------------------------------------------

TEST_F(RpcLifecycleTest, ClosingTheContextFailsAPendingCallPromptly) {
  auto svc = serve<HeldCalc>();
  auto caller = newContext();
  pbtest::CalcProxy calc(*caller);
  calc.init();
  auto result = std::async(std::launch::async, [&] {
    protobus::CallOptions o;
    o.timeoutMs = 4000;
    const auto start = Clock::now();
    try {
      calc.slow(pbtest::SlowRequest(), o);
      return std::make_pair(std::string("returned"), msSince(start));
    } catch (const protobus::DisconnectedError&) {
      return std::make_pair(std::string("disconnected"), msSince(start));
    } catch (const std::exception& e) {
      return std::make_pair(std::string("other: ") + e.what(), msSince(start));
    }
  });
  ASSERT_TRUE(eventually([&] { return svc->entered.load() == 1; }));
  caller->close();
  auto [outcome, elapsed] = result.get();
  EXPECT_EQ(outcome, "disconnected");
  EXPECT_LT(elapsed, 1000);
  // Closing again is harmless.
  EXPECT_NO_THROW(caller->close());
  svc->open();
}

TEST_F(RpcLifecycleTest, AManualDisconnectFailsAPendingCallPromptly) {
  auto svc = serve<HeldCalc>();
  auto caller = newContext();
  pbtest::CalcProxy calc(*caller);
  calc.init();
  auto result = std::async(std::launch::async, [&] {
    protobus::CallOptions o;
    o.timeoutMs = 4000;
    const auto start = Clock::now();
    try {
      calc.slow(pbtest::SlowRequest(), o);
      return std::make_pair(std::string("returned"), msSince(start));
    } catch (const protobus::DisconnectedError&) {
      return std::make_pair(std::string("disconnected"), msSince(start));
    } catch (const std::exception& e) {
      return std::make_pair(std::string("other: ") + e.what(), msSince(start));
    }
  });
  ASSERT_TRUE(eventually([&] { return svc->entered.load() == 1; }));
  caller->connection().disconnect();
  auto [outcome, elapsed] = result.get();
  EXPECT_EQ(outcome, "disconnected");
  EXPECT_LT(elapsed, 1000);
  // A manual disconnect is not a network failure: nothing reconnects.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(caller->connection().isReconnecting());
  EXPECT_FALSE(caller->connection().isConnected());
  svc->open();
}

TEST_F(RpcLifecycleTest, ClosingTheContextEndsABlockedStream) {
  serve();
  auto caller = newContext();
  pbtest::CalcProxy calc(*caller);
  calc.init();
  pbtest::TickRequest r;
  r.set_count(1000);
  r.set_delay_ms(200);
  auto stream = calc.ticks(r);
  auto result = std::async(std::launch::async, [&] {
    const auto start = Clock::now();
    try {
      while (stream.next()) {
      }
      return std::make_pair(std::string("ended"), msSince(start));
    } catch (const protobus::DisconnectedError&) {
      return std::make_pair(std::string("disconnected"), msSince(start));
    } catch (const std::exception& e) {
      return std::make_pair(std::string("other: ") + e.what(), msSince(start));
    }
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  const auto closedAt = Clock::now();
  caller->close();
  auto [outcome, elapsed] = result.get();
  (void)elapsed;
  EXPECT_EQ(outcome, "disconnected");
  EXPECT_LT(msSince(closedAt), 1000);
}

TEST_F(RpcLifecycleTest, ClosingTheContextReleasesABufferedStream) {
  serve();
  auto caller = newContext();
  pbtest::CalcProxy calc(*caller);
  calc.init();
  pbtest::TickRequest r;
  r.set_count(5);
  auto stream = calc.ticks(r);
  // Let the whole stream arrive and sit in the buffer, unread.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  caller->close();
  EXPECT_THROW(stream.next(), protobus::DisconnectedError);
  stream.cancel();
}

TEST_F(RpcLifecycleTest, CloseRacingCompletionSettlesEveryCallOnce) {
  serve();
  for (int round = 0; round < 20; ++round) {
    auto caller = newContext();
    pbtest::CalcProxy calc(*caller);
    calc.init();
    std::vector<std::future<std::string>> calls;
    for (int i = 0; i < 8; ++i) {
      calls.push_back(std::async(std::launch::async, [&calc, i] {
        pbtest::AddRequest r;
        r.set_a(i);
        r.set_b(1);
        protobus::CallOptions o;
        o.timeoutMs = 3000;
        try {
          return std::to_string(calc.add(r, o).result());
        } catch (const protobus::DisconnectedError&) {
          return std::string("disconnected");
        } catch (const protobus::NotConnectedError&) {
          return std::string("not-connected");
        } catch (const protobus::NotReadyError&) {
          return std::string("not-ready");
        } catch (const protobus::PublishError& e) {
          return std::string(e.ambiguous() ? "ambiguous" : "definite");
        } catch (const std::exception& e) {
          return std::string("other: ") + e.what();
        }
      }));
    }
    std::this_thread::sleep_for(std::chrono::microseconds(500 * (round % 4)));
    const auto start = Clock::now();
    caller->close();
    caller->close();
    for (int i = 0; i < 8; ++i) {
      ASSERT_EQ(calls[i].wait_for(std::chrono::seconds(2)), std::future_status::ready);
      const auto v = calls[i].get();
      EXPECT_TRUE(v == std::to_string(i + 1) || v == "disconnected" || v == "not-connected" ||
                  v == "not-ready" || v == "ambiguous")
          << v;
    }
    EXPECT_LT(msSince(start), 2000);
  }
}

// ---- pending state racing a disconnect -------------------------------------------------

// A call registered as the connection drops must neither leave its deadline
// timer behind nor wait out that deadline.
TEST_F(RpcLifecycleTest, CallsRacingADisconnectLeaveNoTimersBehind) {
  bindSink();
  auto& scheduler = ctx->connection().scheduler();
  ASSERT_TRUE(eventually([&] { return scheduler.pending() == 0; }));
  std::mt19937 gen{12345};
  for (int i = 0; i < 60; ++i) {
    auto call = std::async(std::launch::async, [&] {
      protobus::CallOptions o;
      o.timeoutMs = 20000;
      const auto start = Clock::now();
      try {
        ctx->publishMessage("x", "REQUEST.sink.call", o);
      } catch (const std::exception&) {
      }
      return msSince(start);
    });
    std::this_thread::sleep_for(std::chrono::microseconds(gen() % 400));
    // A call that only got going after the reconnection is still pending:
    // drop the connection again until it is not.
    for (int kills = 0; kills < 20; ++kills) {
      broker->killConnections();
      if (call.wait_for(std::chrono::milliseconds(300)) == std::future_status::ready) break;
    }
    ASSERT_EQ(call.wait_for(std::chrono::seconds(1)), std::future_status::ready) << "iteration " << i;
    EXPECT_LT(call.get(), 10000);
    ASSERT_TRUE(eventually([&] { return ctx->connection().isReady(); }));
  }
  // Every deadline timer was cancelled with its call.
  EXPECT_TRUE(eventually([&] { return scheduler.pending() == 0; }, std::chrono::seconds(2)))
      << scheduler.pending() << " timer(s) left";
}

}  // namespace
