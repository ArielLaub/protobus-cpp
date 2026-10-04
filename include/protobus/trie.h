// A topic trie with RabbitMQ topic semantics: words separated by dots, `*`
// for exactly one word and `#` for zero or more.
//
// EventListener routes a delivery to every handler whose pattern matches the
// routing key. The broker has already done the same match to deliver it; the
// trie decides which of this process's handlers it was for.
#pragma once

#include <concepts>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace protobus {

template <typename T>
class Trie {
 public:
  Trie() : root_(std::make_unique<Node>("")) {}

  // Register `value` under `pattern`. Two values under one pattern are both
  // kept.
  void add(const std::string& pattern, T value) { root_->addMatch(pattern, std::move(value)); }

  // Every value whose pattern matches `topic`. A value reached through more
  // than one pattern is returned once.
  std::vector<T> match(const std::string& topic) const { return root_->matchTopic(topic); }

 private:
  using Words = std::vector<std::string>;

  static Words split(const std::string& s) {
    Words out;
    size_t start = 0;
    for (;;) {
      const size_t dot = s.find('.', start);
      if (dot == std::string::npos) {
        out.push_back(s.substr(start));
        return out;
      }
      out.push_back(s.substr(start, dot - start));
      start = dot + 1;
    }
  }

  class Node {
   public:
    explicit Node(std::string word)
        : word_(std::move(word)), isWildcard_(word_ == "*" || word_ == "#"), isSuperWildcard_(word_ == "#") {}

    void addMatch(const std::string& pattern, T value) {
      Words tail = split(pattern);
      Node* node = this;
      for (const auto& w : tail) node = node->ensureChild(w);
      // Only the node the pattern ends on carries the value, so a node that
      // is only a step towards a longer pattern does not match.
      node->values_.push_back(std::move(value));
    }

    std::vector<T> matchTopic(const std::string& topic) const {
      std::vector<const Node*> nodes;
      for (const auto& [_, child] : children_) {
        auto found = child->matchTopicDeep(topic, nullptr, false);
        nodes.insert(nodes.end(), found.begin(), found.end());
      }
      std::vector<T> results;
      std::vector<const Node*> seen;
      for (const Node* n : nodes) {
        bool dup = false;
        for (const Node* s : seen) dup = dup || s == n;
        if (dup) continue;
        seen.push_back(n);
        for (const T& v : n->values_) {
          if constexpr (std::equality_comparable<T>) {
            bool present = false;
            for (const T& r : results) present = present || r == v;
            if (present) continue;
          }
          results.push_back(v);
        }
      }
      return results;
    }

   private:
    // The RabbitMQ topic algorithm as the TypeScript reference implements it.
    // `given` is the tail handed down by the caller, or null at the top level
    // (the topic is then split afresh).
    std::vector<const Node*> matchTopicDeep(const std::string& topic, const Words* given, bool reprocess) const {
      Words tail = given ? *given : split(topic);
      std::vector<const Node*> results;
      auto processChild = [&](const Node* child) {
        auto found = child->matchTopicDeep(topic, &tail, child == this);
        results.insert(results.end(), found.begin(), found.end());
      };
      // '#' stands for zero or more words: offer the current word to every
      // child before consuming one. `reprocess` is set only for a
      // super-wildcard re-entering itself, so nothing is processed twice.
      if (isSuperWildcard_ && !reprocess && !children_.empty()) {
        for (const auto& [_, child] : children_) processChild(child.get());
      }

      if (tail.empty()) return results;
      const std::string word = tail.front();
      tail.erase(tail.begin());
      if (!isWildcard_ && word_ != word) return results;

      if (tail.empty()) {
        // A pattern ends here if anything was registered on this node,
        // whether or not longer patterns branch off it.
        if (!values_.empty()) results.push_back(this);
        // A pattern ending in '#' also ends here.
        auto hash = children_.find("#");
        if (hash != children_.end()) {
          auto found = hash->second->matchTopicDeep(topic, given, false);
          results.insert(results.end(), found.begin(), found.end());
        }
        return results;
      }

      for (const auto& [_, child] : children_) processChild(child.get());
      if (isSuperWildcard_) processChild(this);
      return results;
    }

    Node* ensureChild(const std::string& word) {
      auto it = children_.find(word);
      if (it != children_.end()) return it->second.get();
      auto child = std::make_unique<Node>(word);
      Node* raw = child.get();
      children_.emplace(word, std::move(child));
      return raw;
    }

    std::string word_;
    std::vector<T> values_;
    std::map<std::string, std::unique_ptr<Node>> children_;
    bool isWildcard_;
    bool isSuperWildcard_;
  };

  std::unique_ptr<Node> root_;
};

}  // namespace protobus
