// The TypeScript trie suites, ported case for case: event routing must match
// the same topics on every port.
#include <gtest/gtest.h>

#include <algorithm>

#include "protobus/trie.h"

namespace protobus {
namespace {

std::vector<std::string> sorted(std::vector<std::string> v) {
  std::sort(v.begin(), v.end());
  return v;
}

using S = std::vector<std::string>;

TEST(Trie, ExactSimpleMatch) {
  Trie<std::string> trie;
  trie.add("a.b.c", "abc");
  trie.add("b.c.d", "2");
  EXPECT_EQ(trie.match("a.b.c"), S{"abc"});
  EXPECT_EQ(trie.match("b.c.d"), S{"2"});
  EXPECT_TRUE(trie.match("c.d.e").empty());
}

TEST(Trie, NodeSplit) {
  Trie<std::string> trie;
  trie.add("a.b.c.2", "2");
  trie.add("a.b.c.1", "1");
  EXPECT_EQ(trie.match("a.b.c.1"), S{"1"});
  EXPECT_EQ(trie.match("a.b.c.2"), S{"2"});
}

TEST(Trie, OnlyCompletePatternsMatch) {
  Trie<std::string> trie;
  trie.add("a.b.c.d", "something");
  EXPECT_TRUE(trie.match("a").empty());
  EXPECT_TRUE(trie.match("a.b").empty());
  EXPECT_TRUE(trie.match("a.b.c").empty());
  EXPECT_EQ(trie.match("a.b.c.d").size(), 1u);
}

TEST(Trie, StarInEveryPosition) {
  Trie<std::string> trie;
  trie.add("*.b.c", "first");
  trie.add("a.*.c", "second");
  trie.add("a.b.*", "third");
  EXPECT_EQ(sorted(trie.match("a.b.c")), (S{"first", "second", "third"}));
  EXPECT_EQ(trie.match("z.b.c"), S{"first"});
  EXPECT_EQ(trie.match("a.z.c"), S{"second"});
  EXPECT_EQ(trie.match("a.b.z"), S{"third"});
}

TEST(Trie, HashReplacesZeroOrMoreWords) {
  Trie<std::string> trie;
  trie.add("#.b.c", "first");
  trie.add("a.#.c", "second");
  trie.add("a.b.#", "third");
  for (const char* t : {"z.b.c", "x.z.b.c", "x.y.z.b.c", "b.c"}) EXPECT_EQ(trie.match(t), S{"first"}) << t;
  for (const char* t : {"a.z.c", "a.x.z.c", "a.x.y.z.c", "a.c"}) EXPECT_EQ(trie.match(t), S{"second"}) << t;
  for (const char* t : {"a.b.z", "a.b.x.z", "a.b.x.y.z", "a.b"}) EXPECT_EQ(trie.match(t), S{"third"}) << t;
  for (const char* t : {"b.b.b", "c.c.c", "a.a.a"}) EXPECT_TRUE(trie.match(t).empty()) << t;
}

TEST(Trie, RabbitBlogPost) {
  Trie<std::string> trie;
  trie.add("a.b.c", "first");
  trie.add("a.*.b.c", "second");
  trie.add("a.#.c", "third");
  trie.add("b.b.c", "forth");
  EXPECT_EQ(trie.match("a.d.d.d.c"), S{"third"});
}

TEST(Trie, RabbitTopicsTutorial) {
  Trie<std::string> trie;
  trie.add("*.orange.*", "Q1");
  trie.add("*.*.rabbit", "Q2");
  trie.add("lazy.#", "Q2");
  EXPECT_EQ(trie.match("quick.orange.rabbit").size(), 2u);
  EXPECT_EQ(trie.match("lazy.orange.elephant").size(), 2u);
  EXPECT_EQ(trie.match("quick.orange.fox"), S{"Q1"});
  EXPECT_EQ(trie.match("lazy.brown.fox"), S{"Q2"});
  EXPECT_EQ(trie.match("lazy.pink.rabbit"), S{"Q2"});
  EXPECT_TRUE(trie.match("orange").empty());
  EXPECT_TRUE(trie.match("quick.brown.fox").empty());
  EXPECT_EQ(trie.match("lazy.orange.male.rabbit"), S{"Q2"});
}

TEST(Trie, KeepsBothHandlersOnOneTopic) {
  Trie<std::string> trie;
  trie.add("EVENT.Order", "a");
  trie.add("EVENT.Order", "b");
  EXPECT_EQ(sorted(trie.match("EVENT.Order")), (S{"a", "b"}));
}

TEST(Trie, KeepsAPatternALongerOneIsAddedBeneath) {
  for (bool shortFirst : {true, false}) {
    Trie<std::string> trie;
    if (shortFirst) trie.add("EVENT.Order", "short");
    trie.add("EVENT.Order.Shipped", "long");
    if (!shortFirst) trie.add("EVENT.Order", "short");
    EXPECT_EQ(trie.match("EVENT.Order"), S{"short"});
    EXPECT_EQ(trie.match("EVENT.Order.Shipped"), S{"long"});
  }
}

TEST(Trie, KeepsIntermediateValuesOutOfUnrelatedMatches) {
  Trie<std::string> trie;
  trie.add("EVENT.Order", "short");
  trie.add("EVENT.Order.Shipped", "long");
  trie.add("EVENT.Invoice.Paid", "other");
  EXPECT_TRUE(trie.match("EVENT.Invoice").empty());
  EXPECT_EQ(trie.match("EVENT.Invoice.Paid"), S{"other"});
}

TEST(Trie, SeveralHandlersUnderAWildcard) {
  Trie<std::string> trie;
  trie.add("EVENT.*", "x");
  trie.add("EVENT.*", "y");
  trie.add("EVENT.Order", "exact");
  EXPECT_EQ(sorted(trie.match("EVENT.Order")), (S{"exact", "x", "y"}));
}

// The worked example of the TypeScript docs (docs/message-flow.md).
TEST(Trie, DocumentedWildcardExample) {
  Trie<std::string> trie;
  trie.add("ORDERS.*.CREATED", "A");
  trie.add("ORDERS.#", "B");
  trie.add("ORDERS.US.*.SHIPPED", "C");
  EXPECT_EQ(sorted(trie.match("ORDERS.US.CREATED")), (S{"A", "B"}));
  EXPECT_EQ(trie.match("ORDERS.US.123.CREATED"), S{"B"});
  EXPECT_EQ(sorted(trie.match("ORDERS.US.123.SHIPPED")), (S{"B", "C"}));
  EXPECT_EQ(trie.match("ORDERS.EU.456.SHIPPED"), S{"B"});
  EXPECT_EQ(trie.match("ORDERS"), S{"B"});
}

TEST(Trie, NonComparableValuesAreKeptPerPattern) {
  Trie<std::shared_ptr<int>> trie;
  auto a = std::make_shared<int>(1);
  trie.add("x.*", a);
  trie.add("x.#", a);
  // Two patterns holding the same object: returned once.
  EXPECT_EQ(trie.match("x.y").size(), 1u);
}

}  // namespace
}  // namespace protobus
