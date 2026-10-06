// A consumer the broker cancels while the connection stays up (its queue was
// deleted, say): the listener notices and puts itself back, unless it was
// being stopped on purpose.
#include <gtest/gtest.h>

#include "helpers.h"

namespace {

using pbtesting::eventually;
using pbtesting::MemoryBus;

class RecoveryTest : public MemoryBus {
 protected:
  void deleteQueue(const std::string& name) {
    auto ch = ctx->connection().openChannel();
    ctx->connection().deleteQueue(ch, name);
    ctx->connection().closeChannel(ch);
  }

  int add(pbtest::CalcProxy& calc, int a, int b, int64_t timeoutMs = 3000) {
    pbtest::AddRequest r;
    r.set_a(a);
    r.set_b(b);
    protobus::CallOptions o;
    o.timeoutMs = timeoutMs;
    return calc.add(r, o).result();
  }
};

class CountingLogger : public protobus::ILogger {
 public:
  void debug(const std::string&) override {}
  void info(const std::string&) override {}
  void warn(const std::string& m) override {
    if (m.find("rebuilding") != std::string::npos) ++rebuilds;
  }
  void error(const std::string&) override {}
  std::atomic<int> rebuilds{0};
};

TEST_F(RecoveryTest, ADeletedServiceQueueIsRedeclaredAndConsumedAgain) {
  serve();
  auto calc = proxy();
  EXPECT_EQ(add(calc, 1, 2), 3);
  deleteQueue("pbtest.Calc");
  ASSERT_TRUE(eventually([&] {
    return broker->queueExists("pbtest.Calc") && broker->consumerCount("pbtest.Calc") == 1;
  })) << "the cancelled consumer was never put back";
  EXPECT_FALSE(broker->bindings("pbtest.Calc", "proto.bus").empty());
  EXPECT_EQ(add(calc, 2, 2), 4);
  ASSERT_TRUE(ctx->connection().isReady());
}

TEST_F(RecoveryTest, ADeletedReplyQueueIsReplaced) {
  serve();
  auto calc = proxy();
  EXPECT_EQ(add(calc, 1, 1), 2);
  // Learn the reply queue's server-assigned name from a request's replyTo.
  {
    auto ch = ctx->connection().openChannel();
    protobus::QueueOptions q;
    ctx->connection().declareQueue(ch, "sink", q);
    ctx->connection().bindQueue(ch, "sink", "proto.bus", "REQUEST.sink.#");
    ctx->connection().closeChannel(ch);
  }
  protobus::CallOptions o;
  o.timeoutMs = 20;
  EXPECT_THROW(ctx->publishMessage("x", "REQUEST.sink.call", o), protobus::RpcTimeoutError);
  const auto requests = broker->peek("sink");
  ASSERT_EQ(requests.size(), 1u);
  const std::string replyQueue = requests[0].properties.replyTo.value_or("");
  ASSERT_FALSE(replyQueue.empty());
  ASSERT_EQ(broker->consumerCount(replyQueue), 1u);

  deleteQueue(replyQueue);
  // Calls work again once a new reply queue is in place.
  ASSERT_TRUE(eventually([&] {
    try {
      return add(calc, 5, 5, 300) == 10;
    } catch (const std::exception&) {
      return false;
    }
  }, std::chrono::seconds(5)));
}

TEST_F(RecoveryTest, ADeletedEventQueueIsRedeclaredWithItsBindings) {
  auto l = std::make_shared<protobus::EventListener>(ctx->connectionPtr(), ctx->factoryPtr());
  l->init(nullptr, "recover.Events");
  std::atomic<int> n{0};
  l->subscribe<pbtest::Ping>([&](const pbtest::Ping&, const std::string&, const std::string&) { ++n; });
  l->start();
  deleteQueue("recover.Events");
  ASSERT_TRUE(eventually([&] { return broker->consumerCount("recover.Events") == 1; }));
  EXPECT_EQ(broker->bindings("recover.Events", "proto.bus.events"), std::vector<std::string>{"EVENT.pbtest.Ping"});
  pbtest::Ping p;
  p.set_id("back");
  ctx->publishEvent(p);
  ASSERT_TRUE(eventually([&] { return n.load() == 1; }));
  // start() agrees the listener is running.
  EXPECT_THROW(l->start(), protobus::AlreadyStartedError);
  l->close();
}

TEST_F(RecoveryTest, RecoveryThatKeepsFailingBacksOffThenSucceeds) {
  serve();
  auto calc = proxy();
  auto logger = std::make_shared<CountingLogger>();
  protobus::setLogger(logger);
  protobus::setLogLevel(protobus::LogLevel::Warn);
  // Something redeclares the queue with other arguments the moment it is
  // gone: every recovery attempt now ends in a 406.
  {
    auto ch = ctx->connection().openChannel();
    ctx->connection().deleteQueue(ch, "pbtest.Calc");
    protobus::QueueOptions q;
    q.arguments["x-message-ttl"] = protobus::amqp::FieldValue::fromInt(42);
    ctx->connection().declareQueue(ch, "pbtest.Calc", q);
    ctx->connection().closeChannel(ch);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  const int attempts = logger->rebuilds.load();
  // 100, 200, 400 ms apart: a handful, not a tight loop.
  EXPECT_GE(attempts, 1);
  EXPECT_LE(attempts, 8);
  EXPECT_EQ(broker->consumerCount("pbtest.Calc"), 0u);
  // The conflict goes away; the next attempt succeeds.
  deleteQueue("pbtest.Calc");
  ASSERT_TRUE(eventually([&] { return broker->consumerCount("pbtest.Calc") == 1; }, std::chrono::seconds(5)));
  protobus::setLogLevel(protobus::LogLevel::Silent);
  protobus::setLogger(nullptr);
  EXPECT_EQ(add(calc, 4, 4), 8);
  // Exactly one consumer: no duplicate from overlapping attempts.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_EQ(broker->consumerCount("pbtest.Calc"), 1u);
}

TEST_F(RecoveryTest, AnIntentionalStopIsNotUndone) {
  auto l = std::make_shared<protobus::EventListener>(ctx->connectionPtr(), ctx->factoryPtr());
  l->init(nullptr, "stopped.Events");
  l->subscribe<pbtest::Ping>([](const pbtest::Ping&, const std::string&, const std::string&) {});
  l->start();
  l->stopConsuming();
  deleteQueue("stopped.Events");
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_FALSE(broker->queueExists("stopped.Events"));
  l->close();
}

TEST_F(RecoveryTest, ACancellationRacingCloseDoesNotResurrectTheListener) {
  for (int i = 0; i < 10; ++i) {
    const std::string queue = "race" + std::to_string(i) + ".Events";
    auto l = std::make_shared<protobus::EventListener>(ctx->connectionPtr(), ctx->factoryPtr());
    l->init(nullptr, queue);
    l->subscribe<pbtest::Ping>([](const pbtest::Ping&, const std::string&, const std::string&) {});
    l->start();
    deleteQueue(queue);
    if (i % 2) std::this_thread::sleep_for(std::chrono::milliseconds(i * 20));
    l->close();
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    EXPECT_EQ(broker->consumerCount(queue), 0u) << queue;
  }
}

TEST_F(RecoveryTest, ACancellationRacingShutdownDoesNotResurrectTheService) {
  protobus::RunnableService::setExitAfterShutdown(false);
  auto svc = protobus::RunnableService::start<pbtesting::CalcService>(*ctx);
  deleteQueue("pbtest.Calc");
  protobus::RunnableService::requestShutdown("test");
  EXPECT_EQ(protobus::RunnableService::wait(), 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_EQ(broker->consumerCount("pbtest.Calc"), 0u);
}

}  // namespace
