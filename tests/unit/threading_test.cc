// The threading model's guard rails: opt-in serialized handlers, blocking
// calls refused on threads that must not block, and a Context destroyed
// under running handlers reported loudly.
#include <gtest/gtest.h>

#include <future>

#include "helpers.h"
#include "scheduler.h"

namespace {

using pbtesting::eventually;
using pbtesting::MemoryBus;

using ThreadingTest = MemoryBus;

// Records how many of its handlers run at once, across requests, stream
// steps and events.
class OverlapCalc : public pbtesting::CalcService {
 public:
  using CalcService::CalcService;
  std::atomic<int> active{0};
  std::atomic<int> maxActive{0};
  std::atomic<int> runs{0};

  void enter() {
    const int now = ++active;
    int seen = maxActive.load();
    while (now > seen && !maxActive.compare_exchange_weak(seen, now)) {
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
    --active;
    ++runs;
  }

  pbtest::Nothing slow(const pbtest::SlowRequest&, protobus::CallContext&) override {
    enter();
    return {};
  }

  protobus::Generator<pbtest::Tick> ticks(const pbtest::TickRequest& r, protobus::CallContext&) override {
    for (int i = 0; i < r.count(); ++i) {
      enter();
      pbtest::Tick t;
      t.set_seq(i);
      co_yield t;
    }
  }

  void subscribe() {
    subscribeEvent<pbtest::Ping>([this](const pbtest::Ping&, const std::string&, const std::string&) { enter(); });
  }
};

int runMixedLoad(protobus::Context& callerCtx, OverlapCalc& svc, protobus::Context& eventCtx) {
  pbtest::CalcProxy calc(callerCtx);
  calc.init();
  std::vector<std::future<void>> work;
  for (int i = 0; i < 6; ++i) {
    work.push_back(std::async(std::launch::async, [&calc] { calc.slow(pbtest::SlowRequest()); }));
  }
  work.push_back(std::async(std::launch::async, [&calc] {
    pbtest::TickRequest r;
    r.set_count(4);
    for (const auto& t : calc.ticks(r)) (void)t;
  }));
  for (int i = 0; i < 4; ++i) {
    pbtest::Ping p;
    p.set_id("e" + std::to_string(i));
    eventCtx.publishEvent(p);
  }
  for (auto& w : work) w.get();
  EXPECT_TRUE(eventually([&] { return svc.runs.load() == 6 + 4 + 4; }));
  return svc.maxActive.load();
}

TEST_F(ThreadingTest, SerializedHandlersNeverOverlap) {
  protobus::MessageServiceOptions o;
  o.maxConcurrent = 8;
  o.serializeHandlers = true;
  auto svc = serve<OverlapCalc>(o);
  svc->subscribe();
  auto caller = newContext();
  EXPECT_EQ(runMixedLoad(*caller, *svc, *ctx), 1);
}

// The default is unchanged: handlers run in parallel up to the prefetch.
TEST_F(ThreadingTest, HandlersRunInParallelByDefault) {
  protobus::MessageServiceOptions o;
  o.maxConcurrent = 8;
  auto svc = serve<OverlapCalc>(o);
  svc->subscribe();
  auto caller = newContext();
  EXPECT_GT(runMixedLoad(*caller, *svc, *ctx), 1);
}

// A serialized handler waiting on its own service would wait for itself.
class SelfCaller : public pbtesting::CalcService {
 public:
  using CalcService::CalcService;
  std::string outcome;
  pbtest::Nothing slow(const pbtest::SlowRequest&, protobus::CallContext&) override {
    pbtest::CalcProxy self(context());
    self.init();
    pbtest::AddRequest r;
    r.set_a(1);
    r.set_b(1);
    try {
      protobus::CallOptions o;
      o.timeoutMs = 2000;
      self.add(r, o);
      outcome = "returned";
    } catch (const std::logic_error& e) {
      outcome = e.what();
    } catch (const std::exception& e) {
      outcome = std::string("other: ") + e.what();
    }
    // Another service, even one named under this one, is fine.
    pbtest::CalcProxy sibling(context(), "pbtest.Calc.sibling");
    sibling.init();
    try {
      protobus::CallOptions o;
      o.timeoutMs = 2000;
      sibling.add(r, o);
      outcome += " | sibling ok";
    } catch (const std::exception& e) {
      outcome += std::string(" | sibling: ") + e.what();
    }
    return {};
  }
};

TEST_F(ThreadingTest, ASerializedHandlerCallingItsOwnServiceIsRefused) {
  protobus::MessageServiceOptions o;
  o.serializeHandlers = true;
  auto svc = serve<SelfCaller>(o);
  auto sibling = std::make_shared<pbtesting::InstanceCalc>(*ctx, "pbtest.Calc.sibling");
  sibling->init();
  services.push_back(sibling);
  auto calc = proxy();
  const auto start = std::chrono::steady_clock::now();
  calc.slow(pbtest::SlowRequest());
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(1));
  EXPECT_NE(svc->outcome.find("would deadlock"), std::string::npos) << svc->outcome;
  EXPECT_NE(svc->outcome.find("| sibling ok"), std::string::npos) << svc->outcome;
}

// Without serialization the same call is allowed (and answered).
TEST_F(ThreadingTest, AParallelHandlerMayCallItsOwnService) {
  protobus::MessageServiceOptions o;
  o.maxConcurrent = 2;
  auto svc = serve<SelfCaller>(o);
  auto sibling = std::make_shared<pbtesting::InstanceCalc>(*ctx, "pbtest.Calc.sibling");
  sibling->init();
  services.push_back(sibling);
  auto calc = proxy();
  calc.slow(pbtest::SlowRequest());
  EXPECT_EQ(svc->outcome, "returned | sibling ok");
}

// ---- blocking calls on threads that must not block --------------------------------

std::string whatIsThrown(const std::function<void()>& fn) {
  try {
    fn();
    return "returned";
  } catch (const std::logic_error& e) {
    return std::string("logic_error: ") + e.what();
  } catch (const std::exception& e) {
    return std::string("other: ") + e.what();
  }
}

TEST_F(ThreadingTest, ABlockingCallInOnDisconnectedIsRefused) {
  std::promise<std::string> got;
  auto future = got.get_future();
  std::atomic<bool> once{false};
  const auto id = ctx->connection().onDisconnected([&] {
    if (once.exchange(true)) return;
    got.set_value(whatIsThrown([&] { ctx->publishMessage("x", "REQUEST.nobody.home"); }));
  });
  broker->killConnections();
  ASSERT_EQ(future.wait_for(std::chrono::seconds(2)), std::future_status::ready) << "the callback hung";
  const auto outcome = future.get();
  EXPECT_NE(outcome.find("logic_error"), std::string::npos) << outcome;
  EXPECT_NE(outcome.find("must not block"), std::string::npos) << outcome;
  ctx->connection().removeListener(id);
  ASSERT_TRUE(eventually([&] { return ctx->connection().isReady(); }));
}

TEST_F(ThreadingTest, ABlockingCallOnTheTimerThreadIsRefused) {
  std::promise<std::string> got;
  auto future = got.get_future();
  auto ch = ctx->connection().openChannel();
  ctx->connection().scheduler().schedule(std::chrono::milliseconds(1), [&] {
    got.set_value(whatIsThrown([&] { ctx->connection().publish(ch, "", "nowhere", "x", {}); }));
  });
  ASSERT_EQ(future.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  const auto outcome = future.get();
  EXPECT_NE(outcome.find("logic_error"), std::string::npos) << outcome;
  EXPECT_NE(outcome.find("timer thread"), std::string::npos) << outcome;
}

TEST_F(ThreadingTest, ABlockingCallInAPublishCompletionIsRefused) {
  std::promise<std::string> got;
  auto future = got.get_future();
  auto ch = ctx->connection().openChannel();
  ctx->connection().publishAsync(ch, "", "nowhere", "x", {}, [&](std::exception_ptr) {
    got.set_value(whatIsThrown([&] {
      pbtest::Ping p;
      ctx->publishEvent(p);
    }));
  });
  ASSERT_EQ(future.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  EXPECT_NE(future.get().find("logic_error"), std::string::npos);
}

// The same calls are fine from a thread of the application's own.
TEST_F(ThreadingTest, BlockingCallsWorkFromApplicationThreads) {
  auto ch = ctx->connection().openChannel();
  std::string outcome;
  std::thread([&] { outcome = whatIsThrown([&] { ctx->connection().publish(ch, "", "nowhere", "x", {}); }); }).join();
  EXPECT_EQ(outcome, "returned");
}

// ---- a Context destroyed under running handlers ------------------------------------

class Parked : public pbtesting::CalcService {
 public:
  using CalcService::CalcService;
  std::mutex m;
  std::condition_variable cv;
  bool release = false;
  std::atomic<bool> entered{false};
  pbtest::Nothing slow(const pbtest::SlowRequest&, protobus::CallContext&) override {
    entered = true;
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

class CapturingLogger : public protobus::ILogger {
 public:
  void debug(const std::string&) override {}
  void info(const std::string&) override {}
  void warn(const std::string&) override {}
  void error(const std::string& m) override {
    std::lock_guard<std::mutex> lock(mutex);
    errors.push_back(m);
  }
  std::mutex mutex;
  std::vector<std::string> errors;
};

#ifndef NDEBUG
TEST_F(ThreadingTest, DestroyingAContextUnderARunningHandlerAbortsInDebugBuilds) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(
      {
        env.set("SHUTDOWN_DRAIN_TIMEOUT_MS", "50");
        protobus::setLogLevel(protobus::LogLevel::Error);
        auto doomed = newContext();
        auto svc = std::make_shared<Parked>(*doomed);
        svc->init();
        std::thread([&] {
          try {
            pbtest::CalcProxy calc(*ctx);
            calc.init();
            calc.slow(pbtest::SlowRequest());
          } catch (...) {
          }
        }).detach();
        if (!eventually([&] { return svc->entered.load(); })) std::_Exit(3);
        doomed.reset();
      },
      "being destroyed while 1 handler");
}
#else
TEST_F(ThreadingTest, DestroyingAContextUnderARunningHandlerIsLoggedInReleaseBuilds) {
  env.set("SHUTDOWN_DRAIN_TIMEOUT_MS", "50");
  auto logger = std::make_shared<CapturingLogger>();
  protobus::setLogger(logger);
  protobus::setLogLevel(protobus::LogLevel::Error);
  auto doomed = newContext();
  auto svc = std::make_shared<Parked>(*doomed);
  svc->init();
  std::thread caller([&] {
    try {
      pbtest::CalcProxy calc(*ctx);
      calc.init();
      protobus::CallOptions o;
      o.timeoutMs = 2000;
      calc.slow(pbtest::SlowRequest(), o);
    } catch (...) {
    }
  });
  ASSERT_TRUE(eventually([&] { return svc->entered.load(); }));
  doomed.reset();
  svc->open();
  caller.join();
  protobus::setLogLevel(protobus::LogLevel::Silent);
  protobus::setLogger(nullptr);
  std::lock_guard<std::mutex> lock(logger->mutex);
  bool found = false;
  for (const auto& e : logger->errors) found = found || e.find("being destroyed while 1 handler") != std::string::npos;
  EXPECT_TRUE(found);
}
#endif

// The ordinary order raises nothing.
TEST_F(ThreadingTest, AContextDestroyedAfterItsHandlersIsQuiet) {
  auto logger = std::make_shared<CapturingLogger>();
  protobus::setLogger(logger);
  protobus::setLogLevel(protobus::LogLevel::Error);
  {
    auto c = newContext();
    auto svc = std::make_shared<pbtesting::CalcService>(*c);
    svc->init();
    pbtest::CalcProxy calc(*c);
    calc.init();
    pbtest::AddRequest r;
    r.set_a(2);
    r.set_b(2);
    EXPECT_EQ(calc.add(r).result(), 4);
  }
  protobus::setLogLevel(protobus::LogLevel::Silent);
  protobus::setLogger(nullptr);
  std::lock_guard<std::mutex> lock(logger->mutex);
  for (const auto& e : logger->errors) EXPECT_EQ(e.find("being destroyed"), std::string::npos) << e;
}

}  // namespace
