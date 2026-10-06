// The confirm channel's bookkeeping, frame by frame: which publish a
// basic.return belongs to, and what each basic.ack/nack (single or
// `multiple`) settles. Replayed here in the order RabbitMQ sends the frames.
#include <gtest/gtest.h>

#include <map>

#include "amqp/confirm_tracker.h"

namespace {

using protobus::amqp::ConfirmOutcome;
using protobus::amqp::FieldValue;
using protobus::amqp::Properties;
using protobus::amqp::detail::ConfirmTracker;

class ConfirmTrackerTest : public ::testing::Test {
 protected:
  // Publish with `messageId`, recording what the frame on the wire carries.
  uint64_t publish(const std::string& messageId, bool mandatory = true) {
    Properties p;
    if (!messageId.empty()) p.messageId = messageId;
    auto prepared = tracker.prepare(p, mandatory);
    const uint64_t seq = ++nextSeq;
    sent[seq] = prepared.properties;
    tracker.track(seq, std::move(prepared), [this, seq](ConfirmOutcome o, std::string) {
      outcomes[seq].push_back(o);
    });
    return seq;
  }

  // The broker returns publish `seq`: it echoes the properties it was sent.
  bool returns(uint64_t seq) { return tracker.onReturn(sent.at(seq)); }

  void ack(uint64_t tag, bool multiple = false) {
    for (auto& s : tracker.settle(tag, multiple, false)) s.callback(s.outcome, "");
  }
  void nack(uint64_t tag, bool multiple = false) {
    for (auto& s : tracker.settle(tag, multiple, true)) s.callback(s.outcome, "");
  }

  ConfirmOutcome only(uint64_t seq) {
    EXPECT_EQ(outcomes[seq].size(), 1u) << "publish " << seq;
    return outcomes[seq].empty() ? ConfirmOutcome::Closed : outcomes[seq].front();
  }

  bool tagged(uint64_t seq) {
    const auto& h = sent.at(seq).headers;
    return h && h->count(ConfirmTracker::kTagHeader) > 0;
  }

  ConfirmTracker tracker;
  uint64_t nextSeq = 0;
  std::map<uint64_t, Properties> sent;
  std::map<uint64_t, std::vector<ConfirmOutcome>> outcomes;
};

TEST_F(ConfirmTrackerTest, AUniqueMessageIdTravelsUntouched) {
  const auto a = publish("a");
  EXPECT_FALSE(sent[a].headers.has_value());
  EXPECT_EQ(sent[a].messageId, "a");
  ack(a);
  EXPECT_EQ(only(a), ConfirmOutcome::Ack);
}

TEST_F(ConfirmTrackerTest, SharedIdRoutableThenUnroutable) {
  const auto a = publish("order-1");  // routes; its ack trails
  const auto b = publish("order-1");  // returned
  EXPECT_FALSE(tagged(a));
  EXPECT_TRUE(tagged(b));
  EXPECT_EQ(sent[b].messageId, "order-1");  // the caller's id is kept
  ASSERT_TRUE(returns(b));
  ack(b);
  ack(a);
  EXPECT_EQ(only(a), ConfirmOutcome::Ack);
  EXPECT_EQ(only(b), ConfirmOutcome::Returned);
}

TEST_F(ConfirmTrackerTest, SharedIdUnroutableThenRoutable) {
  const auto a = publish("order-1");  // returned
  const auto b = publish("order-1");  // routes
  ASSERT_TRUE(returns(a));
  ack(b, true);  // one multiple ack for both
  EXPECT_EQ(only(a), ConfirmOutcome::Returned);
  EXPECT_EQ(only(b), ConfirmOutcome::Ack);
}

TEST_F(ConfirmTrackerTest, BothUnroutable) {
  const auto a = publish("dup");
  const auto b = publish("dup");
  const auto c = publish("dup");
  ASSERT_TRUE(returns(a));
  ASSERT_TRUE(returns(b));
  ASSERT_TRUE(returns(c));
  ack(c, true);
  for (auto s : {a, b, c}) EXPECT_EQ(only(s), ConfirmOutcome::Returned);
}

TEST_F(ConfirmTrackerTest, BatchedMultipleAcksKeepEachVerdict) {
  std::vector<uint64_t> seqs;
  for (int i = 0; i < 30; ++i) seqs.push_back(publish("batch-" + std::to_string(i % 4)));
  for (int i = 0; i < 30; i += 3) ASSERT_TRUE(returns(seqs[i]));
  ack(seqs[9], true);
  ack(seqs[10]);
  ack(seqs[29], true);
  for (int i = 0; i < 30; ++i) {
    EXPECT_EQ(only(seqs[i]), i % 3 == 0 ? ConfirmOutcome::Returned : ConfirmOutcome::Ack) << i;
  }
  EXPECT_EQ(tracker.outstanding(), 0u);
}

TEST_F(ConfirmTrackerTest, NoMessageIdIsTagged) {
  const auto a = publish("");
  const auto b = publish("");
  EXPECT_TRUE(tagged(a));
  EXPECT_TRUE(tagged(b));
  ASSERT_TRUE(returns(b));
  ack(b, true);
  EXPECT_EQ(only(a), ConfirmOutcome::Ack);
  EXPECT_EQ(only(b), ConfirmOutcome::Returned);
}

TEST_F(ConfirmTrackerTest, NotMandatoryIsNeverTaggedOrReturned) {
  const auto a = publish("x", false);
  const auto b = publish("x", false);
  EXPECT_FALSE(tagged(a));
  EXPECT_FALSE(tagged(b));
  EXPECT_FALSE(returns(a));
  ack(b, true);
  EXPECT_EQ(only(a), ConfirmOutcome::Ack);
  EXPECT_EQ(only(b), ConfirmOutcome::Ack);
}

// A delivery's headers are republished on the retry path: a tag copied from
// one is stale and must not survive into the new publish.
TEST_F(ConfirmTrackerTest, AStaleTagIsStripped) {
  Properties p;
  p.messageId = "fresh";
  p.headers = protobus::amqp::FieldTable{};
  (*p.headers)[ConfirmTracker::kTagHeader] = FieldValue::fromString("stale-tag");
  (*p.headers)["x-retry-count"] = FieldValue::fromInt(1);
  auto prepared = tracker.prepare(p, true);
  EXPECT_EQ(prepared.properties.headers->count(ConfirmTracker::kTagHeader), 0u);
  EXPECT_EQ(prepared.properties.headers->count("x-retry-count"), 1u);
  // A return naming a tag nobody holds matches nothing, rather than falling
  // back to the messageId.
  tracker.track(1, std::move(prepared), [](ConfirmOutcome, std::string) {});
  EXPECT_FALSE(tracker.onReturn(p));
}

// An id becomes free for an untagged publish again once its holder settles.
TEST_F(ConfirmTrackerTest, AnIdIsReusableAfterSettlement) {
  const auto a = publish("again");
  ack(a);
  const auto b = publish("again");
  EXPECT_FALSE(tagged(b));
  ASSERT_TRUE(returns(b));
  ack(b);
  EXPECT_EQ(only(a), ConfirmOutcome::Ack);
  EXPECT_EQ(only(b), ConfirmOutcome::Returned);
}

TEST_F(ConfirmTrackerTest, ANackIsANackEvenWhenReturned) {
  const auto a = publish("n");
  ASSERT_TRUE(returns(a));
  nack(a);
  EXPECT_EQ(only(a), ConfirmOutcome::Nack);
}

// A caller that timed out does not end the transport's record: the late
// return and ack still settle the publish, exactly once, and a duplicate or
// unknown ack is ignored.
TEST_F(ConfirmTrackerTest, LateOutcomesSettleOnceAndUnknownTagsAreIgnored) {
  const auto a = publish("late");
  const auto b = publish("late");
  EXPECT_EQ(tracker.outstanding(), 2u);
  ASSERT_TRUE(returns(b));
  ack(b, true);
  ack(b, true);
  ack(999);
  EXPECT_EQ(only(a), ConfirmOutcome::Ack);
  EXPECT_EQ(only(b), ConfirmOutcome::Returned);
  EXPECT_FALSE(returns(a));
}

TEST_F(ConfirmTrackerTest, ClosingHandsBackEverythingOutstanding) {
  publish("c1");
  publish("c1");
  publish("");
  auto callbacks = tracker.takeAll();
  EXPECT_EQ(callbacks.size(), 3u);
  EXPECT_EQ(tracker.outstanding(), 0u);
  // Indices are cleared with them: a reused id is untagged again.
  const auto d = publish("c1");
  EXPECT_FALSE(tagged(d));
}

}  // namespace
