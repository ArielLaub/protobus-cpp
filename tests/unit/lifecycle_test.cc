#include <gtest/gtest.h>

#include "helpers.h"

namespace {

using pbtesting::eventually;
using pbtesting::MemoryBus;

using LifecycleTest = MemoryBus;

// The whole graceful shutdown, as a signal would run it: stop taking work,
// drain what is in hand, clean up, close the connection.
TEST_F(LifecycleTest, ShutdownDrainsThenCleansUpThenCloses) {
  protobus::RunnableService::setExitAfterShutdown(false);
  auto svc = protobus::RunnableService::start<pbtesting::CalcService>(*ctx);
  auto calc = proxy();
  std::thread caller([&] {
    pbtest::SlowRequest r;
    r.set_ms(200);
    calc.slow(r);
  });
  ASSERT_TRUE(eventually([&] { return svc->slowStarted.load() == 1; }));
  protobus::RunnableService::requestShutdown("test");
  // The request in hand is answered before the connection closes.
  caller.join();
  EXPECT_EQ(protobus::RunnableService::wait(), 0);
  EXPECT_TRUE(svc->cleanedUp.load());
  EXPECT_FALSE(ctx->isConnected());
  EXPECT_EQ(broker->consumerCount("pbtest.Calc"), 0u);
}

// A service whose slow() holds a resource that cleanup() destroys, and
// records whether cleanup ever ran while the handler was still using it.
class ResourceService : public pbtesting::CalcService {
 public:
  using CalcService::CalcService;
  std::mutex m;
  std::condition_variable cv;
  bool release = false;
  std::atomic<bool> inHandler{false};
  std::atomic<bool> cleanupRaced{false};
  std::atomic<bool> resourceAlive{true};
  // Set in a death test's child: cleanup ends the process with this code.
  int exitInCleanup = 0;

  pbtest::Nothing slow(const pbtest::SlowRequest&, protobus::CallContext&) override {
    inHandler = true;
    std::unique_lock<std::mutex> lock(m);
    cv.wait(lock, [&] { return release; });
    // Still using what cleanup() releases.
    if (!resourceAlive.load()) cleanupRaced = true;
    inHandler = false;
    return {};
  }
  void cleanup() override {
    if (exitInCleanup != 0) std::_Exit(exitInCleanup);
    if (inHandler.load()) cleanupRaced = true;
    resourceAlive = false;
    cleanedUp = true;
  }
  void open() {
    {
      std::lock_guard<std::mutex> lock(m);
      release = true;
    }
    cv.notify_all();
  }
};

// A handler still running at the drain deadline keeps its resources: cleanup
// is deferred until it returns, and the shutdown itself is not held up.
TEST_F(LifecycleTest, CleanupWaitsForAHandlerThatOutlivesTheDrain) {
  env.set("SHUTDOWN_DRAIN_TIMEOUT_MS", "100");
  protobus::RunnableService::setExitAfterShutdown(false);
  auto svc = protobus::RunnableService::start<ResourceService>(*ctx);
  auto callerCtx = newContext();
  pbtest::CalcProxy calc(*callerCtx);
  calc.init();
  std::thread caller([&] {
    protobus::CallOptions o;
    o.timeoutMs = 3000;
    try {
      calc.slow(pbtest::SlowRequest(), o);
    } catch (...) {
    }
  });
  ASSERT_TRUE(eventually([&] { return svc->inHandler.load(); }));
  const auto start = std::chrono::steady_clock::now();
  protobus::RunnableService::requestShutdown("test");
  EXPECT_EQ(protobus::RunnableService::wait(), 0);
  // Bounded: the shutdown finished without waiting for the handler.
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(2));
  EXPECT_TRUE(svc->inHandler.load());
  EXPECT_FALSE(svc->cleanedUp.load());
  EXPECT_FALSE(ctx->isConnected());

  svc->open();
  ASSERT_TRUE(eventually([&] { return svc->cleanedUp.load(); }));
  EXPECT_FALSE(svc->cleanupRaced.load());
  callerCtx->close();
  caller.join();
}

// With the exit on, a handler that never returns ends in a bounded forced
// exit, and cleanup never runs underneath it. The child exits 42 if it does,
// and 7 if wait() hands control back to a main() about to tear down what the
// handler still uses.
TEST_F(LifecycleTest, AForcedShutdownExitsWithoutCleaningUpUnderAHandler) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_EXIT(
      {
        env.set("SHUTDOWN_DRAIN_TIMEOUT_MS", "100");
        env.set("SHUTDOWN_EXIT_GRACE_MS", "300");
        protobus::RunnableService::setExitAfterShutdown(true);
        auto svc = protobus::RunnableService::start<ResourceService>(*ctx);
        svc->exitInCleanup = 42;
        auto callerCtx = newContext();
        pbtest::CalcProxy calc(*callerCtx);
        calc.init();
        std::thread([&calc] {
          try {
            calc.slow(pbtest::SlowRequest());
          } catch (...) {
          }
        }).detach();
        if (!eventually([&] { return svc->inHandler.load(); })) std::_Exit(3);
        protobus::RunnableService::requestShutdown("test");
        protobus::RunnableService::wait();
        std::_Exit(7);
      },
      ::testing::ExitedWithCode(0), "");
}

// The ordinary path is unchanged when the drain succeeds: cleanup runs after
// the handler, before wait() returns.
TEST_F(LifecycleTest, ADrainedShutdownCleansUpBeforeWaitReturns) {
  env.set("SHUTDOWN_DRAIN_TIMEOUT_MS", "2000");
  protobus::RunnableService::setExitAfterShutdown(false);
  auto svc = protobus::RunnableService::start<ResourceService>(*ctx);
  auto callerCtx = newContext();
  pbtest::CalcProxy calc(*callerCtx);
  calc.init();
  std::thread caller([&] {
    try {
      calc.slow(pbtest::SlowRequest());
    } catch (...) {
    }
  });
  ASSERT_TRUE(eventually([&] { return svc->inHandler.load(); }));
  protobus::RunnableService::requestShutdown("test");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(svc->cleanedUp.load());
  svc->open();
  EXPECT_EQ(protobus::RunnableService::wait(), 0);
  EXPECT_TRUE(svc->cleanedUp.load());
  EXPECT_FALSE(svc->cleanupRaced.load());
  caller.join();
}

TEST_F(LifecycleTest, AFailedStartClosesTheConnectionAndRethrows) {
  class Broken : public pbtesting::CalcService {
   public:
    using CalcService::CalcService;
    std::string ServiceName() const override { return "missing.Service"; }
  };
  auto c = newContext();
  EXPECT_THROW(protobus::RunnableService::start<Broken>(*c), protobus::MissingProto);
  EXPECT_FALSE(c->isConnected());
}

TEST_F(LifecycleTest, ProtoFileNameFollowsThePackage) {
  pbtesting::CalcService s(*ctx);
  EXPECT_EQ(s.ProtoFileName(), "pbtest.proto");
}

}  // namespace
