// A processing timeout fires the handler's signal. Application abort
// listeners must not run on the connection's shared timer thread: one that
// blocks would stall every other timer in the process.
#include <gtest/gtest.h>

#include "helpers.h"
#include "scheduler.h"

namespace {

using pbtesting::eventually;
using pbtesting::MemoryBus;

using Clock = std::chrono::steady_clock;
using AbortTimeoutTest = MemoryBus;

class BlockingListenerCalc : public pbtesting::CalcService {
 public:
  using CalcService::CalcService;
  std::mutex m;
  std::condition_variable cv;
  bool release = false;
  std::atomic<bool> listenerEntered{false};
  std::atomic<bool> listenerDone{false};
  std::atomic<bool> handlerSawAbort{false};
  std::atomic<int64_t> abortSeenAfterMs{-1};

  pbtest::Nothing slow(const pbtest::SlowRequest& r, protobus::CallContext& ctx) override {
    const auto start = Clock::now();
    ctx.signal.addListener([this] {
      listenerEntered = true;
      // A "short" listener contending for a lock someone else holds.
      std::unique_lock<std::mutex> lock(m);
      cv.wait(lock, [&] { return release; });
      listenerDone = true;
    });
    // Waiters on the signal wake as soon as it fires, whatever its listeners
    // are doing.
    if (ctx.signal.waitFor(std::chrono::milliseconds(r.ms()))) {
      handlerSawAbort = true;
      abortSeenAfterMs =
          std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count();
    }
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

TEST_F(AbortTimeoutTest, ABlockedAbortListenerDoesNotStallOtherTimers) {
  protobus::MessageServiceOptions o;
  o.processingTimeoutMs = 30;
  o.retry.maxRetries = 0;
  auto svc = serve<BlockingListenerCalc>(o);
  auto callerCtx = newContext();
  pbtest::CalcProxy calc(*callerCtx);
  calc.init();

  std::atomic<bool> answered{false};
  std::string answer;
  std::thread caller([&] {
    pbtest::SlowRequest r;
    r.set_ms(5000);
    protobus::CallOptions co;
    co.timeoutMs = 4000;
    try {
      calc.slow(r, co);
      answer = "returned";
    } catch (const protobus::RemoteError& e) {
      answer = e.code();
    } catch (const std::exception& e) {
      answer = std::string("other: ") + e.what();
    }
    answered = true;
  });

  ASSERT_TRUE(eventually([&] { return svc->listenerEntered.load(); }));
  // An unrelated timer on the same scheduler.
  std::atomic<bool> fired{false};
  ctx->connection().scheduler().schedule(std::chrono::milliseconds(10), [&] { fired = true; });
  const auto start = Clock::now();
  while (!fired.load() && Clock::now() - start < std::chrono::milliseconds(120)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  EXPECT_TRUE(fired.load()) << "the timer thread is stuck in an abort listener";

  // The handler's wait ended promptly, and the caller was answered, while
  // the listener was still blocked.
  EXPECT_TRUE(eventually([&] { return svc->handlerSawAbort.load(); }, std::chrono::milliseconds(500)));
  EXPECT_LT(svc->abortSeenAfterMs.load(), 1000);
  EXPECT_TRUE(eventually([&] { return answered.load(); }, std::chrono::seconds(2)));
  EXPECT_FALSE(svc->listenerDone.load());

  svc->open();
  caller.join();
  EXPECT_EQ(answer, "PROCESSING_TIMEOUT");
  EXPECT_TRUE(eventually([&] { return svc->listenerDone.load(); }));
}

// A handler finishing just as its timeout fires settles exactly once,
// whichever wins, with listeners that take their time.
TEST_F(AbortTimeoutTest, AHandlerRacingItsTimeoutSettlesOnce) {
  class Racer : public pbtesting::CalcService {
   public:
    using CalcService::CalcService;
    std::atomic<int> listeners{0};
    pbtest::Nothing slow(const pbtest::SlowRequest& r, protobus::CallContext& ctx) override {
      ctx.signal.addListener([this] {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        ++listeners;
      });
      std::this_thread::sleep_for(std::chrono::milliseconds(r.ms()));
      return {};
    }
  };
  protobus::MessageServiceOptions o;
  o.processingTimeoutMs = 20;
  o.retry.maxRetries = 0;
  auto svc = serve<Racer>(o);
  auto calc = proxy();
  for (int ms : {15, 18, 19, 20, 20, 21, 21, 22, 25, 18, 19, 20, 21, 22}) {
    pbtest::SlowRequest r;
    r.set_ms(ms);
    protobus::CallOptions co;
    co.timeoutMs = 3000;
    try {
      calc.slow(r, co);
    } catch (const protobus::RemoteError& e) {
      EXPECT_EQ(e.code(), "PROCESSING_TIMEOUT");
    }
  }
  ASSERT_TRUE(eventually([&] { return broker->unackedCount("pbtest.Calc") == 0; }));
  EXPECT_EQ(broker->queueDepth("pbtest.Calc"), 0u);
  // A timed-out handler runs on after its delivery settles; it still ends.
  EXPECT_TRUE(eventually([&] { return ctx->connection().inFlightDeliveries() == 0; }));
}

// The service an abort listener refers to stays alive until the listener is
// done, even when the application lets go of it the moment the caller is
// answered: the listener runs off the timer thread, but never beyond its
// handler's lifetime.
std::atomic<bool> listenerServiceDestroyed{false};
std::atomic<int> listenerSawDestroyed{0};
std::atomic<int> listenerRuns{0};

TEST_F(AbortTimeoutTest, AnAbortListenerNeverOutlivesItsService) {
  class Held : public pbtesting::CalcService {
   public:
    using CalcService::CalcService;
    ~Held() override { listenerServiceDestroyed = true; }
    pbtest::Nothing slow(const pbtest::SlowRequest& r, protobus::CallContext& ctx) override {
      ctx.signal.addListener([] {
        // Still running well after the handler itself has returned.
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        if (listenerServiceDestroyed.load()) ++listenerSawDestroyed;
        ++listenerRuns;
      });
      std::this_thread::sleep_for(std::chrono::milliseconds(r.ms()));
      return {};
    }
  };
  listenerServiceDestroyed = false;
  listenerSawDestroyed = 0;
  listenerRuns = 0;
  protobus::MessageServiceOptions o;
  o.processingTimeoutMs = 20;
  o.retry.maxRetries = 0;
  auto svc = std::make_shared<Held>(*ctx, o);
  svc->init();
  auto calc = proxy();
  pbtest::SlowRequest r;
  r.set_ms(40);
  try {
    calc.slow(r);
  } catch (const protobus::RemoteError& e) {
    EXPECT_EQ(e.code(), "PROCESSING_TIMEOUT");
  }
  // The application's last reference goes while the listener still runs.
  svc.reset();
  ASSERT_TRUE(eventually([&] { return listenerRuns.load() == 1; }));
  EXPECT_EQ(listenerSawDestroyed.load(), 0);
  EXPECT_TRUE(eventually([&] { return listenerServiceDestroyed.load(); }));
}

}  // namespace
