// Real-broker checks for behaviour the in-memory broker can only model: a
// topic permission as the security boundary for retry routing, a consumer
// cancelled by queue deletion, and how RabbitMQ orders returns and confirms.
#include <gtest/gtest.h>

#include <future>
#include <random>

#include "broker.h"
#include "helpers.h"
#include "wire/envelope.h"

namespace {

using pbtesting::eventually;
using protobus::amqp::ConfirmOutcome;

class RabbitHardeningTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto* broker = integration::Broker::require();
    if (broker == nullptr) GTEST_SKIP() << "PROTOBUS_TEST_AMQP_URL and PROTOBUS_TEST_MGMT_URL are not both set";
    env.set("RPC_CALL_TIMEOUT_MS", "20000");
    if (!std::getenv("PROTOBUS_TEST_LOG")) protobus::setLogLevel(protobus::LogLevel::Silent);
    vhost = broker->newVHost();
    ctx = std::make_unique<protobus::Context>();
    protobus::ContextOptions o;
    o.reconnection.initialDelayMs = 100;
    o.reconnection.maxDelayMs = 500;
    ctx->init(vhost->url(), {}, o);
  }

  void TearDown() override {
    services.clear();
    ctx.reset();
    vhost.reset();
    protobus::setLogLevel(protobus::LogLevel::Info);
  }

  pbtesting::ScopedEnv env;
  std::unique_ptr<integration::VHost> vhost;
  std::unique_ptr<protobus::Context> ctx;
  std::vector<std::shared_ptr<protobus::MessageService>> services;
};

std::string uniqueName(const std::string& prefix) {
  static std::mt19937 gen{std::random_device{}()};
  return prefix + std::to_string(gen());
}

// Publish on a raw transport channel and collect the broker's verdict.
struct Verdicts {
  std::mutex m;
  std::condition_variable cv;
  std::map<int, ConfirmOutcome> got;
  void record(int i, ConfirmOutcome o) {
    {
      std::lock_guard<std::mutex> lock(m);
      got[i] = o;
    }
    cv.notify_all();
  }
  bool waitFor(size_t n) {
    std::unique_lock<std::mutex> lock(m);
    return cv.wait_for(lock, std::chrono::seconds(15), [&] { return got.size() >= n; });
  }
};

// The security boundary itself: a publisher whose topic permission allows only
// EVENT.allowed cannot reach the EVENT.privileged handler by forging
// x-original-routing-key and letting the subscriber's retry republish it.
TEST_F(RabbitHardeningTest, AForgedRoutingHeaderCannotCrossATopicPermission) {
  protobus::EventRetryOptions retry;
  retry.maxRetries = 2;
  retry.retryDelayMs = 100;
  auto l = std::make_shared<protobus::EventListener>(ctx->connectionPtr(), ctx->factoryPtr(), retry);
  l->init(nullptr, "secure.Events");
  std::atomic<int> allowed{0};
  std::atomic<int> privileged{0};
  l->subscribe<pbtest::Ping>(
      [&](const pbtest::Ping&, const std::string&, const std::string&) {
        ++allowed;
        throw std::runtime_error("allowed handler failed");
      },
      "EVENT.allowed");
  l->subscribe<pbtest::Ping>([&](const pbtest::Ping&, const std::string&, const std::string&) { ++privileged; },
                             "EVENT.privileged");
  l->start();

  const std::string user = uniqueName("restricted-");
  const std::string url = vhost->createUser(user, "secret", "^$", "^proto\\\\.bus\\\\.events$", "^$",
                                            "proto.bus.events", "^EVENT\\\\.allowed$");
  auto conn = protobus::amqp::rabbitmqTransport()->connect(url, 30);

  pbtest::Ping ping;
  ping.set_id("forged");
  const std::string body = protobus::wire::encodeEvent({"pbtest.Ping", "EVENT.allowed", ping.SerializeAsString()});

  // The permission is real: the restricted user cannot publish to the
  // privileged key directly.
  {
    auto ch = conn->openChannel();
    Verdicts v;
    ch->publish("proto.bus.events", "EVENT.privileged", body, {}, false,
                [&](ConfirmOutcome o, std::string) { v.record(0, o); });
    ASSERT_TRUE(v.waitFor(1));
    EXPECT_EQ(v.got[0], ConfirmOutcome::Closed);
  }

  auto ch = conn->openChannel();
  protobus::amqp::Properties props;
  protobus::amqp::FieldTable forged;
  forged["x-original-routing-key"] = protobus::amqp::FieldValue::fromString("EVENT.privileged");
  props.headers = forged;
  props.messageId = "forged-1";
  Verdicts v;
  ch->publish("proto.bus.events", "EVENT.allowed", body, props, false,
              [&](ConfirmOutcome o, std::string) { v.record(0, o); });
  ASSERT_TRUE(v.waitFor(1));
  ASSERT_EQ(v.got[0], ConfirmOutcome::Ack);

  EXPECT_EQ(vhost->waitQueueDepth("secure.Events.DLQ", 1), 1);
  EXPECT_EQ(allowed.load(), 3);
  EXPECT_EQ(privileged.load(), 0);
  conn->close();
  l->close();
}

// basic.cancel from the broker (here: the queue deleted under the consumer)
// leaves channel and connection open. The listener must notice and recover.
TEST_F(RabbitHardeningTest, AConsumerCancelledByQueueDeletionRecovers) {
  auto svc = std::make_shared<pbtesting::CalcService>(*ctx);
  svc->init();
  services.push_back(svc);
  pbtest::CalcProxy calc(*ctx);
  calc.init();
  pbtest::AddRequest r;
  r.set_a(1);
  r.set_b(2);
  EXPECT_EQ(calc.add(r).result(), 3);

  ASSERT_TRUE(vhost->deleteQueue("pbtest.Calc"));
  EXPECT_TRUE(eventually(
      [&] {
        try {
          protobus::CallOptions o;
          o.timeoutMs = 500;
          return calc.add(r, o).result() == 3;
        } catch (const std::exception&) {
          return false;
        }
      },
      std::chrono::seconds(10)));
  EXPECT_TRUE(vhost->queueExists("pbtest.Calc"));
  EXPECT_TRUE(ctx->connection().isReady());
}

class ReturnCorrelation : public RabbitHardeningTest {
 protected:
  void SetUp() override {
    RabbitHardeningTest::SetUp();
    if (IsSkipped()) return;
    ch = ctx->connection().openChannel();
    protobus::QueueOptions q;  // durable
    ctx->connection().declareQueue(ch, "routable", q);
  }

  void publish(int i, Verdicts& v, const std::string& routingKey, const std::string& messageId) {
    protobus::amqp::Properties props;
    props.messageId = messageId;
    // Persistent to a durable queue: its confirm waits for the disk, so it
    // trails the immediate return of an unroutable neighbour.
    props.deliveryMode = 2;
    ch->publish("", routingKey, std::string(4096, 'x'), props, true,
                [&v, i](ConfirmOutcome o, std::string) { v.record(i, o); });
  }

  std::shared_ptr<protobus::amqp::Channel> ch;
};

// Two outstanding publishes share a stable application id; one routes and one
// does not. Each must get its own verdict.
TEST_F(ReturnCorrelation, ASharedMessageIdDoesNotSpreadAReturn) {
  constexpr int kPairs = 100;
  Verdicts v;
  for (int i = 0; i < kPairs; ++i) {
    const std::string id = "order-" + std::to_string(i);
    publish(2 * i, v, "routable", id);
    publish(2 * i + 1, v, "nowhere", id);
  }
  ASSERT_TRUE(v.waitFor(2 * kPairs));
  int misattributed = 0;
  for (int i = 0; i < kPairs; ++i) {
    if (v.got[2 * i] != ConfirmOutcome::Ack) ++misattributed;
    EXPECT_EQ(v.got[2 * i + 1], ConfirmOutcome::Returned) << i;
  }
  EXPECT_EQ(misattributed, 0) << "routable publishes reported as returned";
}

// The unroutable one first, then the routable one.
TEST_F(ReturnCorrelation, AReturnDoesNotLeakForwardToALaterPublish) {
  constexpr int kPairs = 100;
  Verdicts v;
  for (int i = 0; i < kPairs; ++i) {
    publish(2 * i, v, "nowhere", "same-id");
    publish(2 * i + 1, v, "routable", "same-id");
  }
  ASSERT_TRUE(v.waitFor(2 * kPairs));
  for (int i = 0; i < kPairs; ++i) {
    EXPECT_EQ(v.got[2 * i], ConfirmOutcome::Returned) << i;
    EXPECT_EQ(v.got[2 * i + 1], ConfirmOutcome::Ack) << i;
  }
}

TEST_F(ReturnCorrelation, BothUnroutableAreBothReturned) {
  Verdicts v;
  for (int i = 0; i < 50; ++i) publish(i, v, "nowhere", "dup");
  ASSERT_TRUE(v.waitFor(50));
  for (int i = 0; i < 50; ++i) EXPECT_EQ(v.got[i], ConfirmOutcome::Returned) << i;
}

// Hundreds in flight with mixed ids and routes, which RabbitMQ confirms with
// `multiple`.
TEST_F(ReturnCorrelation, BatchedConfirmsKeepEachVerdict) {
  constexpr int kN = 600;
  Verdicts v;
  for (int i = 0; i < kN; ++i) {
    const bool routable = (i % 3) != 0;
    publish(i, v, routable ? "routable" : "nowhere", "batch-" + std::to_string(i % 5));
  }
  ASSERT_TRUE(v.waitFor(kN));
  for (int i = 0; i < kN; ++i) {
    EXPECT_EQ(v.got[i], (i % 3) != 0 ? ConfirmOutcome::Ack : ConfirmOutcome::Returned) << i;
  }
}

// Through Connection: a definite UnroutableError for exactly the unroutable
// publish, and success for its routable twin, even after timeouts.
TEST_F(ReturnCorrelation, ConnectionReportsUnroutableOnlyForTheReturnedPublish) {
  constexpr int kPairs = 50;
  std::atomic<int> wrong{0};
  std::atomic<int> done{0};
  for (int i = 0; i < kPairs; ++i) {
    for (bool routable : {true, false}) {
      protobus::PublishOptions p;
      p.mandatory = true;
      p.properties.messageId = "shared-" + std::to_string(i);
      p.properties.deliveryMode = 2;
      ctx->connection().publishAsync(ch, "", routable ? "routable" : "nowhere", std::string(4096, 'y'), p,
                                     [&, routable](std::exception_ptr e) {
                                       bool unroutable = false;
                                       try {
                                         if (e) std::rethrow_exception(e);
                                       } catch (const protobus::UnroutableError&) {
                                         unroutable = true;
                                       } catch (...) {
                                       }
                                       if (unroutable == routable) ++wrong;
                                       ++done;
                                     });
    }
  }
  ASSERT_TRUE(eventually([&] { return done.load() == 2 * kPairs; }, std::chrono::seconds(15)));
  EXPECT_EQ(wrong.load(), 0);
}

}  // namespace
