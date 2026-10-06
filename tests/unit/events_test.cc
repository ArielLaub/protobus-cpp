#include <gtest/gtest.h>

#include "helpers.h"
#include "wire/envelope.h"

namespace {

using pbtesting::eventually;
using pbtesting::header;
using pbtesting::MemoryBus;

class EventsTest : public MemoryBus {
 protected:
  std::shared_ptr<protobus::EventListener> listener(const std::string& queue = "",
                                                    protobus::EventRetryOptions retry = {}) {
    auto l = std::make_shared<protobus::EventListener>(ctx->connectionPtr(), ctx->factoryPtr(), retry);
    l->init(nullptr, queue);
    listeners.push_back(l);
    return l;
  }
  std::vector<std::shared_ptr<protobus::EventListener>> listeners;

  void TearDown() override {
    listeners.clear();
    MemoryBus::TearDown();
  }
};

pbtest::Ping ping(const std::string& id) {
  pbtest::Ping p;
  p.set_id(id);
  *p.mutable_n() = protobus::makeBigint(protobus::Uint256::parse("1180591620717411303424"));
  return p;
}

TEST_F(EventsTest, ATypedSubscriberReceivesTheEvent) {
  auto l = listener();
  std::mutex m;
  std::vector<std::string> got;
  l->subscribe<pbtest::Ping>([&](const pbtest::Ping& p, const std::string& type, const std::string& topic) {
    std::lock_guard<std::mutex> lock(m);
    got.push_back(p.id() + "|" + type + "|" + topic + "|" + protobus::toUint256(p.n()).toString());
  });
  l->start();
  ctx->publishEvent(ping("p1"));
  ASSERT_TRUE(eventually([&] {
    std::lock_guard<std::mutex> lock(m);
    return got.size() == 1;
  }));
  EXPECT_EQ(got[0], "p1|pbtest.Ping|EVENT.pbtest.Ping|1180591620717411303424");
}

TEST_F(EventsTest, ADynamicSubscriberReceivesTheFactoryType) {
  auto l = listener();
  std::atomic<int> n{0};
  l->subscribe("pbtest.Ping",
               [&](const google::protobuf::Message& m, const std::string&, const std::string& topic) {
                 EXPECT_EQ(m.GetDescriptor()->full_name(), "pbtest.Ping");
                 EXPECT_EQ(topic, "EVENT.custom.topic");
                 ++n;
               },
               "EVENT.custom.topic");
  l->start();
  ctx->publishEvent(ping("p2"), "EVENT.custom.topic");
  ctx->publishEvent(ping("elsewhere"));  // EVENT.pbtest.Ping: not subscribed here
  ASSERT_TRUE(eventually([&] { return n.load() == 1; }));
  broker->flush();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_EQ(n.load(), 1);
}

TEST_F(EventsTest, RoutingFollowsTheBrokersKeyNotThePublishersBody) {
  auto l = listener();
  std::atomic<int> secret{0};
  std::atomic<int> visible{0};
  l->subscribe<pbtest::Ping>([&](const pbtest::Ping&, const std::string&, const std::string&) { ++secret; },
                             "EVENT.secret");
  l->subscribe<pbtest::Ping>([&](const pbtest::Ping&, const std::string&, const std::string&) { ++visible; },
                             "EVENT.public");
  l->start();
  // The body claims EVENT.secret; the broker delivers it on EVENT.public.
  auto ch = ctx->connection().openChannel();
  const std::string body = protobus::wire::encodeEvent({"pbtest.Ping", "EVENT.secret", ping("x").SerializeAsString()});
  ctx->connection().publish(ch, "proto.bus.events", "EVENT.public", body, {});
  ASSERT_TRUE(eventually([&] { return visible.load() == 1; }));
  broker->flush();
  EXPECT_EQ(secret.load(), 0);
}

TEST_F(EventsTest, SubscribeAllSeesEverything) {
  auto l = listener();
  std::atomic<int> n{0};
  l->subscribeAll([&](const google::protobuf::Message&, const std::string&, const std::string&) { ++n; });
  l->start();
  ctx->publishEvent(ping("a"));
  ctx->publishEvent(ping("b"), "EVENT.other");
  ASSERT_TRUE(eventually([&] { return n.load() == 2; }));
}

TEST_F(EventsTest, AFailingHandlerDropsTheEventByDefault) {
  auto l = listener("events.drop");
  std::atomic<int> attempts{0};
  l->subscribe<pbtest::Ping>([&](const pbtest::Ping&, const std::string&, const std::string&) {
    ++attempts;
    throw std::runtime_error("handler failed");
  });
  l->start();
  ctx->publishEvent(ping("d"));
  ASSERT_TRUE(eventually([&] { return attempts.load() == 1; }));
  ASSERT_TRUE(eventually([&] { return broker->unackedCount("events.drop") == 0; }));
  EXPECT_EQ(broker->queueDepth("events.drop"), 0u);
  EXPECT_FALSE(broker->queueExists("events.drop.Retry"));
}

TEST_F(EventsTest, EventRetryClimbsALadderOfItsOwn) {
  protobus::EventRetryOptions retry;
  retry.maxRetries = 2;
  retry.retryDelayMs = 20;
  auto l = listener("svc.Events", retry);
  ASSERT_TRUE(l->retryTopology());
  EXPECT_EQ(l->retryTopology()->dlq, "svc.Events.DLQ");
  std::atomic<int> attempts{0};
  l->subscribe<pbtest::Ping>([&](const pbtest::Ping&, const std::string&, const std::string&) {
    ++attempts;
    throw std::runtime_error("still failing");
  });
  l->start();
  ctx->publishEvent(ping("r"));
  ASSERT_TRUE(eventually([&] { return broker->queueDepth("svc.Events.DLQ") == 1; }));
  EXPECT_EQ(attempts.load(), 3);
  auto dead = broker->peek("svc.Events.DLQ").at(0);
  EXPECT_EQ(header(dead, "x-retry-count"), "2");
  EXPECT_EQ(header(dead, "x-original-routing-key"), "EVENT.pbtest.Ping");
  // The redelivery is confined to this subscriber's own queue.
  EXPECT_EQ(broker->bindings("svc.Events", "svc.Events.Redelivery"), std::vector<std::string>{"#"});
}

// x-original-routing-key is publisher-controlled. A retry must go back under
// the key the broker delivered, never the header's: otherwise a publisher
// allowed only EVENT.allowed could have this subscriber republish its event
// to the handler for EVENT.privileged, with the subscriber's permissions.
TEST_F(EventsTest, AForgedOriginalRoutingKeyCannotRedirectARetry) {
  protobus::EventRetryOptions retry;
  retry.maxRetries = 2;
  retry.retryDelayMs = 20;
  auto l = listener("forged.Events", retry);
  std::atomic<int> allowed{0};
  std::atomic<int> privileged{0};
  // Distinct typed handlers: the Ping handler on EVENT.allowed fails, and the
  // Pong handler on EVENT.privileged must never see this event.
  l->subscribe<pbtest::Ping>(
      [&](const pbtest::Ping&, const std::string&, const std::string&) {
        ++allowed;
        throw std::runtime_error("allowed handler failed");
      },
      "EVENT.allowed");
  l->subscribe<pbtest::Pong>([&](const pbtest::Pong&, const std::string&, const std::string&) { ++privileged; },
                             "EVENT.privileged");
  l->start();

  auto ch = ctx->connection().openChannel();
  protobus::PublishOptions p;
  protobus::amqp::FieldTable forged;
  forged["x-original-routing-key"] = protobus::amqp::FieldValue::fromString("EVENT.privileged");
  p.properties.headers = forged;
  const std::string body =
      protobus::wire::encodeEvent({"pbtest.Ping", "EVENT.allowed", ping("forged").SerializeAsString()});
  ctx->connection().publish(ch, "proto.bus.events", "EVENT.allowed", body, p);

  ASSERT_TRUE(eventually([&] { return broker->queueDepth("forged.Events.DLQ") == 1; }));
  EXPECT_EQ(allowed.load(), 3);
  EXPECT_EQ(privileged.load(), 0);
  auto dead = broker->peek("forged.Events.DLQ").at(0);
  EXPECT_EQ(dead.routingKey, "forged.Events.DLQ");
  // The header now records the key the message actually travelled under.
  EXPECT_EQ(header(dead, "x-original-routing-key"), "EVENT.allowed");
}

// A Pong delivered on a Ping handler's key fails that handler. Its retries
// stay on that key: they cannot reach the Pong handler bound elsewhere.
TEST_F(EventsTest, ARetriedEventOfAnotherTypeStaysOnItsKey) {
  protobus::EventRetryOptions retry;
  retry.maxRetries = 1;
  retry.retryDelayMs = 20;
  auto l = listener("typed.Events", retry);
  std::atomic<int> pings{0};
  std::atomic<int> pongs{0};
  l->subscribe<pbtest::Ping>([&](const pbtest::Ping&, const std::string&, const std::string&) { ++pings; },
                             "EVENT.allowed");
  l->subscribe<pbtest::Pong>([&](const pbtest::Pong&, const std::string&, const std::string&) { ++pongs; },
                             "EVENT.privileged");
  l->start();
  auto ch = ctx->connection().openChannel();
  protobus::PublishOptions p;
  protobus::amqp::FieldTable forged;
  forged["x-original-routing-key"] = protobus::amqp::FieldValue::fromString("EVENT.privileged");
  p.properties.headers = forged;
  pbtest::Pong pong;
  pong.set_id("sneaky");
  const std::string body = protobus::wire::encodeEvent({"pbtest.Pong", "EVENT.allowed", pong.SerializeAsString()});
  ctx->connection().publish(ch, "proto.bus.events", "EVENT.allowed", body, p);
  // The type mismatch is an unhandled failure: retried once, then dead.
  ASSERT_TRUE(eventually([&] { return broker->queueDepth("typed.Events.DLQ") == 1; }));
  broker->flush();
  EXPECT_EQ(pongs.load(), 0);
  EXPECT_EQ(pings.load(), 0);
}

// Legitimate retries are unchanged: every hop comes back on the same key and
// the counter climbs.
TEST_F(EventsTest, RepeatedRetriesKeepTheirRoutingKey) {
  protobus::EventRetryOptions retry;
  retry.maxRetries = 3;
  retry.retryDelayMs = 10;
  auto l = listener("hops.Events", retry);
  std::mutex m;
  std::vector<std::string> keys;
  l->subscribe<pbtest::Ping>(
      [&](const pbtest::Ping&, const std::string&, const std::string& topic) {
        std::lock_guard<std::mutex> lock(m);
        keys.push_back(topic);
        throw std::runtime_error("again");
      },
      "EVENT.hops");
  l->start();
  ctx->publishEvent(ping("h"), "EVENT.hops");
  ASSERT_TRUE(eventually([&] { return broker->queueDepth("hops.Events.DLQ") == 1; }));
  std::lock_guard<std::mutex> lock(m);
  EXPECT_EQ(keys, std::vector<std::string>(4, "EVENT.hops"));
  auto dead = broker->peek("hops.Events.DLQ").at(0);
  EXPECT_EQ(header(dead, "x-retry-count"), "3");
  EXPECT_EQ(header(dead, "x-original-routing-key"), "EVENT.hops");
}

TEST_F(EventsTest, AHandledErrorIsNotRetried) {
  protobus::EventRetryOptions retry;
  retry.maxRetries = 2;
  retry.retryDelayMs = 20;
  auto l = listener("svc2.Events", retry);
  std::atomic<int> attempts{0};
  l->subscribe<pbtest::Ping>([&](const pbtest::Ping&, const std::string&, const std::string&) {
    ++attempts;
    throw protobus::HandledError("not for me");
  });
  l->start();
  ctx->publishEvent(ping("h"));
  ASSERT_TRUE(eventually([&] { return attempts.load() == 1; }));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(attempts.load(), 1);
  EXPECT_EQ(broker->queueDepth("svc2.Events.DLQ"), 0u);
}

TEST_F(EventsTest, AServiceSubscribesThroughItsOwnQueue) {
  auto svc = serve();
  std::atomic<int> n{0};
  svc->subscribeEvent<pbtest::Pong>([&](const pbtest::Pong& p, const std::string&, const std::string&) {
    EXPECT_EQ(p.id(), "pong");
    ++n;
  });
  pbtest::Pong pong;
  pong.set_id("pong");
  svc->publishEvent(pong);
  ASSERT_TRUE(eventually([&] { return n.load() == 1; }));
  EXPECT_EQ(broker->bindings("pbtest.Calc.Events", "proto.bus.events"),
            std::vector<std::string>{"EVENT.pbtest.Pong"});
}

TEST_F(EventsTest, AnEventOfAnotherTypeFailsATypedHandler) {
  auto l = listener("events.typed");
  std::atomic<int> n{0};
  l->subscribe<pbtest::Ping>([&](const pbtest::Ping&, const std::string&, const std::string&) { ++n; },
                             "EVENT.shared");
  l->start();
  pbtest::Pong pong;
  ctx->publishEvent(pong, "EVENT.shared");
  ASSERT_TRUE(eventually([&] { return broker->unackedCount("events.typed") == 0 && broker->queueDepth("events.typed") == 0; }));
  EXPECT_EQ(n.load(), 0);
}

TEST_F(EventsTest, PublishingRequiresTheTypeToMatch) {
  EXPECT_THROW(ctx->publishEvent("pbtest.Pong", ping("x")), protobus::InvalidMessageError);
}

}  // namespace
