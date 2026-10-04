#include <gtest/gtest.h>

#include "helpers.h"

namespace {

using pbtesting::eventually;
using pbtesting::MemoryBus;
using protobus::testing::MemoryBroker;

class ConnectionTest : public MemoryBus {
 protected:
  std::shared_ptr<protobus::amqp::Channel> channelWithQueue(const std::string& queue) {
    auto ch = ctx->connection().openChannel();
    ctx->connection().declareExchange(ch, "test.x", "direct");
    protobus::QueueOptions q;
    ctx->connection().declareQueue(ch, queue, q);
    ctx->connection().bindQueue(ch, queue, "test.x", queue);
    return ch;
  }
};

TEST_F(ConnectionTest, APublishReturnsOnceConfirmed) {
  auto ch = channelWithQueue("q1");
  protobus::PublishOptions p;
  p.mandatory = true;
  const std::string id = ctx->connection().publish(ch, "test.x", "q1", "hello", p);
  EXPECT_FALSE(id.empty());
  ASSERT_EQ(broker->queueDepth("q1"), 1u);
  EXPECT_EQ(broker->peek("q1")[0].properties.messageId, id);
}

TEST_F(ConnectionTest, ACallerSuppliedMessageIdIsKept) {
  auto ch = channelWithQueue("q1");
  protobus::PublishOptions p;
  p.properties.messageId = "stable";
  EXPECT_EQ(ctx->connection().publish(ch, "test.x", "q1", "x", p), "stable");
}

TEST_F(ConnectionTest, ARefusedPublishIsDefinite) {
  auto ch = channelWithQueue("q1");
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Nack);
  try {
    ctx->connection().publish(ch, "test.x", "q1", "x", {});
    FAIL();
  } catch (const protobus::PublishNackedError& e) {
    EXPECT_FALSE(e.ambiguous());
    EXPECT_FALSE(e.messageId().empty());
  }
}

TEST_F(ConnectionTest, AMissingConfirmIsAmbiguous) {
  env.set("PUBLISH_CONFIRM_TIMEOUT_MS", "50");
  auto ch = channelWithQueue("q1");
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Drop);
  try {
    ctx->connection().publish(ch, "test.x", "q1", "x", {});
    FAIL();
  } catch (const protobus::PublishConfirmTimeoutError& e) {
    EXPECT_TRUE(e.ambiguous());
  }
}

TEST_F(ConnectionTest, AMandatoryPublishToNothingIsUnroutable) {
  auto ch = channelWithQueue("q1");
  protobus::PublishOptions p;
  p.mandatory = true;
  EXPECT_THROW(ctx->connection().publish(ch, "test.x", "nowhere", "x", p), protobus::UnroutableError);
  // Not mandatory: an unroutable message is simply confirmed.
  EXPECT_NO_THROW(ctx->connection().publish(ch, "test.x", "nowhere", "x", {}));
}

TEST_F(ConnectionTest, APublishLostWithItsChannelIsAmbiguous) {
  auto ch = channelWithQueue("q1");
  try {
    ctx->connection().publish(ch, "no.such.exchange", "x", "x", {});
    FAIL();
  } catch (const protobus::ChannelClosedError& e) {
    EXPECT_TRUE(e.ambiguous());
    EXPECT_NE(std::string(e.what()).find("404"), std::string::npos);
  }
}

TEST_F(ConnectionTest, OutstandingConfirmsAreBoundedAndReleasedWhenTheChannelCloses) {
  env.set("MAX_OUTSTANDING_CONFIRMS", "2");
  env.set("PUBLISH_CONFIRM_TIMEOUT_MS", "10000");
  auto ch = channelWithQueue("q1");
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Drop);
  std::atomic<int> done{0};
  std::atomic<int> closed{0};
  for (int i = 0; i < 4; ++i) {
    ctx->connection().publishAsync(ch, "test.x", "q1", "x", {}, [&](std::exception_ptr e) {
      try {
        if (e) std::rethrow_exception(e);
      } catch (const protobus::ChannelClosedError&) {
        ++closed;
      } catch (...) {
      }
      ++done;
    });
  }
  // Two were sent; two wait for a slot.
  EXPECT_EQ(broker->queueDepth("q1"), 2u);
  ch->close();
  ASSERT_TRUE(eventually([&] { return done.load() == 4; }));
  EXPECT_EQ(closed.load(), 4);
}

TEST_F(ConnectionTest, ServicesComeBackAfterTheConnectionDrops) {
  serve();
  auto calc = proxy();
  pbtest::AddRequest r;
  r.set_a(1);
  r.set_b(2);
  EXPECT_EQ(calc.add(r).result(), 3);
  std::atomic<int> reconnected{0};
  ctx->connection().onReconnected([&] { ++reconnected; });

  broker->killConnections();
  ASSERT_TRUE(eventually([&] { return reconnected.load() == 1; }));
  EXPECT_TRUE(ctx->connection().isReady());
  // The service's queue, bindings and consumer are back, and so is the
  // caller's reply queue.
  EXPECT_EQ(calc.add(r).result(), 3);
  EXPECT_EQ(broker->consumerCount("pbtest.Calc"), 1u);
}

TEST_F(ConnectionTest, ACallMadeDuringTheOutageWaitsForTheReconnection) {
  serve();
  auto calc = proxy();
  broker->refuseConnections(true);
  broker->killConnections();
  ASSERT_TRUE(eventually([&] { return !ctx->connection().isReady(); }));
  std::thread restore([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    broker->refuseConnections(false);
  });
  pbtest::AddRequest r;
  r.set_a(2);
  r.set_b(2);
  EXPECT_EQ(calc.add(r).result(), 4);
  restore.join();
}

TEST_F(ConnectionTest, ACallInFlightWhenTheConnectionDropsIsDisconnected) {
  protobus::MessageServiceOptions o;
  o.retry.maxRetries = 0;
  serve(o);
  auto calc = proxy();
  std::thread killer([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    broker->killConnections();
  });
  pbtest::SlowRequest r;
  r.set_ms(1000);
  EXPECT_THROW(calc.slow(r), protobus::DisconnectedError);
  killer.join();
}

TEST_F(ConnectionTest, AStreamInFlightWhenTheConnectionDropsIsDisconnected) {
  serve();
  auto calc = proxy();
  pbtest::TickRequest r;
  r.set_count(100);
  r.set_delay_ms(20);
  auto stream = calc.ticks(r);
  std::thread killer([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    broker->killConnections();
  });
  EXPECT_THROW(
      {
        while (stream.next()) {
        }
      },
      protobus::DisconnectedError);
  killer.join();
}

TEST_F(ConnectionTest, GivingUpFailsWaitersWithTheReason) {
  protobus::ContextOptions o;
  o.reconnection.maxRetries = 2;
  o.reconnection.initialDelayMs = 5;
  o.reconnection.maxDelayMs = 10;
  auto c = newContext(o);
  std::atomic<int> errors{0};
  c->connection().onError([&](const std::exception&) { ++errors; });
  broker->refuseConnections(true);
  broker->killConnections();
  ASSERT_TRUE(eventually([&] { return !c->connection().isReady(); }));
  try {
    c->connection().whenReady(2000);
    FAIL();
  } catch (const protobus::NotReadyError& e) {
    EXPECT_NE(std::string(e.what()).find("max reconnection attempts (2) exceeded"), std::string::npos);
  }
  EXPECT_GE(errors.load(), 1);
  broker->refuseConnections(false);
}

TEST_F(ConnectionTest, AClosedConnectionIsNotReady) {
  auto c = newContext();
  c->connection().disconnect();
  EXPECT_THROW(c->connection().whenReady(100), protobus::NotReadyError);
  EXPECT_FALSE(c->isConnected());
}

TEST_F(ConnectionTest, AConnectionCannotBeOpenedTwice) {
  EXPECT_THROW(ctx->connection().connect("amqp://memory/"), protobus::AlreadyConnectedError);
}

TEST_F(ConnectionTest, DrainWaitsForInFlightWork) {
  auto svc = serve();
  auto calc = proxy();
  std::thread caller([&] {
    pbtest::SlowRequest r;
    r.set_ms(150);
    calc.slow(r);
  });
  ASSERT_TRUE(eventually([&] { return svc->slowStarted.load() == 1; }));
  svc->stopConsuming();
  EXPECT_GE(ctx->connection().inFlightDeliveries(), 1u);
  EXPECT_TRUE(ctx->connection().drainInFlight(2000));
  EXPECT_EQ(ctx->connection().inFlightDeliveries(), 0u);
  caller.join();
}

TEST_F(ConnectionTest, DrainReportsWorkStillRunning) {
  auto svc = serve();
  auto calc = proxy();
  std::thread caller([&] {
    pbtest::SlowRequest r;
    r.set_ms(300);
    calc.slow(r);
  });
  ASSERT_TRUE(eventually([&] { return svc->slowStarted.load() == 1; }));
  EXPECT_FALSE(ctx->connection().drainInFlight(20));
  caller.join();
}

TEST_F(ConnectionTest, AListenerRebuildsAChannelLostOnALiveConnection) {
  serve();
  auto calc = proxy();
  broker->closeChannelsConsuming("pbtest.Calc");
  ASSERT_TRUE(eventually([&] { return broker->consumerCount("pbtest.Calc") == 1; }));
  pbtest::AddRequest r;
  r.set_a(5);
  r.set_b(5);
  EXPECT_EQ(calc.add(r).result(), 10);
}

TEST_F(ConnectionTest, AStoppedServiceIsNotRestoredByAReconnection) {
  auto svc = serve();
  svc->stopConsuming();
  std::atomic<int> reconnected{0};
  ctx->connection().onReconnected([&] { ++reconnected; });
  broker->killConnections();
  ASSERT_TRUE(eventually([&] { return reconnected.load() == 1; }));
  broker->flush();
  EXPECT_EQ(broker->consumerCount("pbtest.Calc"), 0u);
}

TEST_F(ConnectionTest, ARedeliveredMessageSaysSo) {
  // Unacknowledged work is requeued when its connection drops, and the
  // handler on the new one sees redelivered.
  auto other = newContext();
  auto svc = serve<pbtesting::CalcService>({}, other.get());
  auto calc = proxy();
  std::thread caller([&] {
    pbtest::SlowRequest r;
    r.set_ms(300);
    try {
      calc.slow(r);
    } catch (...) {
    }
  });
  ASSERT_TRUE(eventually([&] { return svc->slowStarted.load() == 1; }));
  broker->killConnections();
  ASSERT_TRUE(eventually([&] { return svc->slowStarted.load() == 2; }, std::chrono::seconds(5)));
  caller.join();
  services.clear();
}

}  // namespace

namespace {

class CountingLogger : public protobus::ILogger {
 public:
  void info(const std::string&) override {}
  void debug(const std::string&) override {}
  void warn(const std::string& m) override { count(m); }
  void error(const std::string& m) override { count(m); }
  std::atomic<int> rebuilds{0};

 private:
  void count(const std::string& m) {
    if (m.find("rebuild") != std::string::npos) ++rebuilds;
  }
};

TEST_F(ConnectionTest, AChannelThatKeepsFailingIsNotRebuiltInATightLoop) {
  serve();
  auto logger = std::make_shared<CountingLogger>();
  protobus::setLogger(logger);
  protobus::setLogLevel(protobus::LogLevel::Warn);
  {
    auto other = newContext();
    protobus::MessageServiceOptions o;
    o.retry.retryDelayMs = 1234;  // its retry queue redeclaration fails with a 406
    auto second = std::make_shared<pbtesting::CalcService>(*other, o);
    EXPECT_THROW(second->init(), protobus::RetryQueueMismatchError);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  protobus::setLogLevel(protobus::LogLevel::Silent);
  protobus::setLogger(nullptr);
  EXPECT_LE(logger->rebuilds.load(), 5);
}

TEST_F(ConnectionTest, ManyPublishesParkedOnTheBoundUnwindWhenTheConnectionDrops) {
  env.set("PUBLISH_CONFIRM_TIMEOUT_MS", "60000");
  auto ch = channelWithQueue("q1");
  broker->setConfirmMode(MemoryBroker::ConfirmMode::Drop);
  constexpr int kPublishes = 20000;
  std::atomic<int> done{0};
  for (int i = 0; i < kPublishes; ++i) {
    ctx->connection().publishAsync(ch, "test.x", "q1", "x", {}, [&](std::exception_ptr) { ++done; });
  }
  broker->killConnections();
  ASSERT_TRUE(eventually([&] { return done.load() == kPublishes; }, std::chrono::seconds(20)));
}

}  // namespace

namespace {

TEST_F(ConnectionTest, ARebuildThatKeepsFailingBacksOff) {
  serve();
  // An operator redeclares the retry queue with other arguments: every
  // rebuild of the service's channel now ends in a 406.
  auto ch = ctx->connection().openChannel();
  ctx->connection().deleteQueue(ch, "pbtest.Calc.Retry");
  protobus::QueueOptions q;
  q.arguments["x-message-ttl"] = protobus::amqp::FieldValue::fromInt(42);
  ctx->connection().declareQueue(ch, "pbtest.Calc.Retry", q);

  auto logger = std::make_shared<CountingLogger>();
  protobus::setLogger(logger);
  protobus::setLogLevel(protobus::LogLevel::Warn);
  broker->closeChannelsConsuming("pbtest.Calc");
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  protobus::setLogLevel(protobus::LogLevel::Silent);
  protobus::setLogger(nullptr);
  // 100, 200, 400 ms apart: a handful of attempts, not thousands.
  EXPECT_LE(logger->rebuilds.load(), 8);
  EXPECT_GE(logger->rebuilds.load(), 2);
}

}  // namespace
