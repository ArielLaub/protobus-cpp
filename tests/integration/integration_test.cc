// The real-broker suite: everything the in-memory broker stands in for,
// checked against RabbitMQ through rabbitmq-c.
#include <gtest/gtest.h>

#include <future>

#include "broker.h"
#include "helpers.h"

namespace {

using pbtesting::CalcService;
using pbtesting::eventually;
using pbtesting::header;

class RabbitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto* broker = integration::Broker::require();
    if (broker == nullptr) GTEST_SKIP() << "PROTOBUS_TEST_AMQP_URL and PROTOBUS_TEST_MGMT_URL are not both set";
    env.set("RPC_CALL_TIMEOUT_MS", "20000");
    env.set("STREAM_IDLE_TIMEOUT_MS", "20000");
    if (!std::getenv("PROTOBUS_TEST_LOG")) protobus::setLogLevel(protobus::LogLevel::Silent);
    vhost = broker->newVHost();
    ctx = newContext();
  }

  void TearDown() override {
    services.clear();
    extra.clear();
    ctx.reset();
    vhost.reset();
    protobus::setLogLevel(protobus::LogLevel::Info);
  }

  std::unique_ptr<protobus::Context> newContext() {
    auto c = std::make_unique<protobus::Context>();
    protobus::ContextOptions o;
    o.reconnection.initialDelayMs = 100;
    o.reconnection.maxDelayMs = 500;
    o.reconnection.maxRetries = 50;
    c->init(vhost->url(), {}, o);
    return c;
  }

  template <typename T = CalcService>
  std::shared_ptr<T> serve(protobus::MessageServiceOptions options = {}) {
    auto s = std::make_shared<T>(*ctx, options);
    s->init();
    services.push_back(s);
    return s;
  }

  pbtest::CalcProxy proxy(const std::string& name = pbtest::CalcProxy::kServiceName) {
    pbtest::CalcProxy p(*ctx, name);
    p.init();
    return p;
  }

  // Drain a queue through a plain consumer, returning what it held.
  std::vector<protobus::amqp::Delivery> drain(const std::string& queue, size_t want) {
    auto ch = ctx->connection().openChannel();
    std::mutex m;
    std::vector<protobus::amqp::Delivery> got;
    ch->consume(queue, "drain", true, false,
                [&](protobus::amqp::Delivery d) {
                  std::lock_guard<std::mutex> lock(m);
                  got.push_back(std::move(d));
                },
                nullptr);
    eventually(
        [&] {
          std::lock_guard<std::mutex> lock(m);
          return got.size() >= want;
        },
        std::chrono::seconds(10));
    ch->close();
    return got;
  }

  pbtesting::ScopedEnv env;
  std::unique_ptr<integration::VHost> vhost;
  std::unique_ptr<protobus::Context> ctx;
  std::vector<std::shared_ptr<protobus::MessageService>> services;
  std::vector<std::unique_ptr<protobus::Context>> extra;
};

pbtest::AddRequest add(int a, int b) {
  pbtest::AddRequest r;
  r.set_a(a);
  r.set_b(b);
  return r;
}

TEST_F(RabbitTest, UnaryCallsAndErrors) {
  serve();
  auto calc = proxy();
  EXPECT_EQ(calc.add(add(20, 22)).result(), 42);
  pbtest::DivideRequest d;
  d.set_dividend(1);
  try {
    calc.divide(d);
    FAIL();
  } catch (const protobus::RemoteError& e) {
    EXPECT_EQ(e.code(), "DIVISION_BY_ZERO");
  }
  try {
    calc.unimplemented({});
    FAIL();
  } catch (const protobus::RemoteError& e) {
    EXPECT_EQ(e.code(), "PROTOCOL_ERROR");
  }
}

TEST_F(RabbitTest, RetryLadderEndsInTheDlqWithItsMetadata) {
  protobus::MessageServiceOptions o;
  o.retry.maxRetries = 2;
  o.retry.retryDelayMs = 100;
  auto svc = serve(o);
  auto calc = proxy();
  pbtest::FailRequest f;
  f.set_id("ladder");
  protobus::CallOptions call;
  call.messageId = "ladder-1";
  try {
    calc.fail(f, call);
    FAIL();
  } catch (const protobus::RemoteError& e) {
    EXPECT_STREQ(e.what(), "boom ladder");
  }
  EXPECT_EQ(svc->failAttempts.load(), 3);
  ASSERT_EQ(vhost->waitQueueDepth("pbtest.Calc.DLQ", 1), 1);
  auto dead = drain("pbtest.Calc.DLQ", 1);
  ASSERT_EQ(dead.size(), 1u);
  EXPECT_EQ(header(dead[0], "x-retry-count"), "2");
  EXPECT_EQ(header(dead[0], "x-original-queue"), "pbtest.Calc");
  EXPECT_EQ(header(dead[0], "x-original-routing-key"), "REQUEST.pbtest.Calc.fail");
  EXPECT_EQ(header(dead[0], "x-last-error"), "std::runtime_error");
  EXPECT_EQ(dead[0].properties.messageId, "ladder-1");
  EXPECT_EQ(dead[0].properties.contentType, "application/octet-stream");
}

TEST_F(RabbitTest, ProcessingTimeoutAnswersTheCaller) {
  protobus::MessageServiceOptions o;
  o.retry.maxRetries = 0;
  o.processingTimeoutMs = 100;
  serve(o);
  auto calc = proxy();
  pbtest::SlowRequest r;
  r.set_ms(3000);
  try {
    calc.slow(r);
    FAIL();
  } catch (const protobus::RemoteError& e) {
    EXPECT_EQ(e.code(), "PROCESSING_TIMEOUT");
  }
}

TEST_F(RabbitTest, StreamsInOrderAndCancels) {
  auto svc = serve();
  auto calc = proxy();
  pbtest::TickRequest r;
  r.set_count(200);
  int i = 0;
  for (const auto& t : calc.ticks(r)) EXPECT_EQ(t.seq(), i++);
  EXPECT_EQ(i, 200);

  r.set_count(500);
  r.set_delay_ms(10);
  int n = 0;
  {
    auto stream = calc.ticks(r);
    for (const auto& t : stream) {
      (void)t;
      if (++n == 3) break;
    }
  }
  ASSERT_TRUE(eventually([&] { return svc->stoppedEarly.load(); }, std::chrono::seconds(10)));
  EXPECT_FALSE(svc->finished.load());
}

TEST_F(RabbitTest, LargeMessagesSpanManyFrames) {
  serve();
  auto calc = proxy();
  pbtest::Wallet w;
  // Well past the 128 KiB frame size: the body arrives in many frames.
  for (int i = 0; i < 40000; ++i) *w.add_parts() = protobus::makeBigint(protobus::Uint256(i));
  auto back = calc.echo(w);
  ASSERT_EQ(back.parts_size(), 40000);
  EXPECT_EQ(protobus::toUint256(back.parts(39999)), protobus::Uint256(39999));
}

TEST_F(RabbitTest, ConcurrentCallsShareTheService) {
  protobus::MessageServiceOptions o;
  o.maxConcurrent = 8;
  serve(o);
  auto calc = proxy();
  std::vector<std::future<int>> results;
  for (int i = 0; i < 64; ++i) {
    results.push_back(std::async(std::launch::async, [&calc, i] { return calc.add(add(i, i)).result(); }));
  }
  for (int i = 0; i < 64; ++i) EXPECT_EQ(results[i].get(), 2 * i);
}

TEST_F(RabbitTest, PublishOutcomes) {
  auto ch = ctx->connection().openChannel();
  protobus::PublishOptions mandatory;
  mandatory.mandatory = true;
  EXPECT_THROW(ctx->connection().publish(ch, "proto.bus", "REQUEST.nobody.here", "x", mandatory),
               protobus::UnroutableError);
  // The channel survives an unroutable publish.
  EXPECT_NO_THROW(ctx->connection().publish(ch, "proto.bus", "REQUEST.nobody.here", "x", {}));
  try {
    ctx->connection().publish(ch, "no.such.exchange", "k", "x", {});
    FAIL();
  } catch (const protobus::ChannelClosedError& e) {
    EXPECT_TRUE(e.ambiguous());
    EXPECT_NE(std::string(e.what()).find("404"), std::string::npos);
  }
  EXPECT_FALSE(ch->isOpen());
  // The connection, and every other channel, is unaffected.
  serve();
  EXPECT_EQ(proxy().add(add(1, 1)).result(), 2);
}

TEST_F(RabbitTest, AChangedRetryDelayIsAMismatchNotAnOpaque406) {
  serve();
  auto other = newContext();
  protobus::MessageServiceOptions o;
  o.retry.retryDelayMs = 777;
  auto second = std::make_shared<CalcService>(*other, o);
  EXPECT_THROW(second->init(), protobus::RetryQueueMismatchError);
  // The 406 closed one channel, not the connection.
  EXPECT_TRUE(other->isConnected());
  extra.push_back(std::move(other));
}

TEST_F(RabbitTest, EventsWithCustomTypes) {
  auto l = std::make_shared<protobus::EventListener>(ctx->connectionPtr(), ctx->factoryPtr());
  l->init(nullptr, "");
  std::promise<std::string> got;
  l->subscribe<pbtest::Ping>([&](const pbtest::Ping& p, const std::string&, const std::string&) {
    got.set_value(protobus::toUint256(p.n()).toString());
  });
  l->start();
  pbtest::Ping p;
  p.set_id("e1");
  *p.mutable_n() = protobus::makeBigint("1180591620717411303424");
  ctx->publishEvent(p);
  auto f = got.get_future();
  ASSERT_EQ(f.wait_for(std::chrono::seconds(10)), std::future_status::ready);
  EXPECT_EQ(f.get(), "1180591620717411303424");
  l->close();
}

TEST_F(RabbitTest, PriorityQueueIsDeclared) {
  protobus::MessageServiceOptions o;
  o.maxPriority = 2;
  serve(o);
  protobus::CallOptions call;
  call.priority = protobus::Config::PRIORITY_HIGH;
  EXPECT_EQ(proxy().add(add(2, 3), call).result(), 5);
}

TEST_F(RabbitTest, RecoversFromABrokerSideDisconnect) {
  serve();
  auto calc = proxy();
  EXPECT_EQ(calc.add(add(1, 2)).result(), 3);
  std::atomic<int> reconnected{0};
  ctx->connection().onReconnected([&] { ++reconnected; });
  ASSERT_GE(vhost->closeConnections(), 1);
  ASSERT_TRUE(eventually([&] { return reconnected.load() >= 1; }, std::chrono::seconds(20)));
  EXPECT_EQ(calc.add(add(3, 4)).result(), 7);
  pbtest::TickRequest r;
  r.set_count(5);
  int n = 0;
  for (const auto& t : calc.ticks(r)) {
    (void)t;
    ++n;
  }
  EXPECT_EQ(n, 5);
}

TEST_F(RabbitTest, AHeartbeatInTheUrlIsHonoured) {
  auto c = std::make_unique<protobus::Context>();
  c->init(vhost->url() + "?heartbeat=5", {});
  serve();
  pbtest::CalcProxy p(*c);
  p.init();
  EXPECT_EQ(p.add(add(1, 1)).result(), 2);
}

TEST_F(RabbitTest, WrongCredentialsFailToConnect) {
  protobus::Context c;
  const std::string url = vhost->url();
  const auto at = url.find('@');
  const std::string bad = "amqp://nobody:wrong" + url.substr(at);
  EXPECT_THROW(c.init(bad, {}), protobus::amqp::AmqpError);
}

}  // namespace
