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
