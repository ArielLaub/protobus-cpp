// Matches a confirm channel's broker answers to the publishes they answer.
//
// Confirms name a publish by its sequence number; basic.return does not name
// it at all, only echoing the message. RabbitMQ sends a mandatory publish's
// return before its confirm, so the return marks the publish and the confirm
// (possibly one `multiple` ack covering many) reports it as Returned.
//
// The return must be matched to the right publish. The application messageId
// is not enough on its own: it is a deduplication identity, and outstanding
// publishes may legitimately share it (a caller republishing after an
// ambiguous failure). Misreading one publish's return as another's verdict
// reports a routed message as UnroutableError, a DEFINITE failure the caller
// may well retry. So a mandatory publish whose messageId is unique among the
// channel's outstanding mandatory publishes is matched by messageId, leaving
// it untouched on the wire; one that would be ambiguous (a shared id, or none)
// carries a per-publish tag header instead, and its return is matched by tag.
//
// Not thread-safe: owned by one channel's I/O thread.
#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "protobus/transport.h"
#include "uuid.h"

namespace protobus::amqp::detail {

class ConfirmTracker {
 public:
  // Private to protobus, and harmless to every port: consumers copy unknown
  // headers through or ignore them.
  static constexpr const char* kTagHeader = "x-protobus-publish-tag";

  struct Prepared {
    Properties properties;  // what to send
    std::string messageId;
    std::string tag;  // empty when matched by messageId
    bool mandatory = false;
  };

  struct Settled {
    ConfirmCallback callback;
    ConfirmOutcome outcome;
  };

  ConfirmTracker() : prefix_(protobus::detail::randomUuid() + "-") {}

  // The properties to publish with. Call track() with the result once the
  // publish has been written, and not at all when it could not be.
  Prepared prepare(const Properties& properties, bool mandatory) {
    Prepared p;
    p.properties = properties;
    p.mandatory = mandatory;
    p.messageId = properties.messageId.value_or("");
    // A tag copied in from a republished delivery's headers is stale.
    if (p.properties.headers) p.properties.headers->erase(kTagHeader);
    if (!mandatory) return p;
    if (p.messageId.empty() || untagged_.count(p.messageId) > 0) {
      p.tag = prefix_ + std::to_string(++nextTag_);
      if (!p.properties.headers) p.properties.headers = FieldTable{};
      (*p.properties.headers)[kTagHeader] = FieldValue::fromString(p.tag);
    }
    return p;
  }

  void track(uint64_t seq, Prepared prepared, ConfirmCallback callback) {
    Entry e;
    e.callback = std::move(callback);
    e.messageId = std::move(prepared.messageId);
    e.tag = std::move(prepared.tag);
    e.mandatory = prepared.mandatory;
    if (e.mandatory) {
      if (!e.tag.empty()) {
        tagged_[e.tag] = seq;
      } else {
        untagged_[e.messageId] = seq;
      }
    }
    pending_.emplace(seq, std::move(e));
  }

  // A basic.return. Returns whether it matched an outstanding publish.
  bool onReturn(const Properties& returned) {
    uint64_t seq = 0;
    bool found = false;
    if (returned.headers) {
      auto h = returned.headers->find(kTagHeader);
      if (h != returned.headers->end()) {
        if (auto tag = h->second.asString()) {
          auto it = tagged_.find(*tag);
          if (it != tagged_.end()) {
            seq = it->second;
            found = true;
          }
        }
        // A tag that matches nothing is not ours to guess at.
        if (!found) return false;
      }
    }
    if (!found && returned.messageId) {
      auto it = untagged_.find(*returned.messageId);
      if (it != untagged_.end()) {
        seq = it->second;
        found = true;
      }
    }
    if (!found) return false;
    auto p = pending_.find(seq);
    if (p == pending_.end()) return false;
    p->second.returned = true;
    return true;
  }

  // basic.ack or basic.nack. The callbacks are returned rather than called,
  // so the caller can run them after its own bookkeeping.
  std::vector<Settled> settle(uint64_t deliveryTag, bool multiple, bool nacked) {
    std::vector<Settled> out;
    auto finish = [&](std::map<uint64_t, Entry>::iterator it) {
      Entry& e = it->second;
      ConfirmOutcome outcome = nacked ? ConfirmOutcome::Nack : ConfirmOutcome::Ack;
      if (!nacked && e.returned) outcome = ConfirmOutcome::Returned;
      unindex(it->first, e);
      out.push_back(Settled{std::move(e.callback), outcome});
      return pending_.erase(it);
    };
    if (multiple) {
      for (auto it = pending_.begin(); it != pending_.end() && it->first <= deliveryTag;) it = finish(it);
    } else if (auto it = pending_.find(deliveryTag); it != pending_.end()) {
      finish(it);
    }
    return out;
  }

  // Every outstanding publish, removed: for a channel that has closed.
  std::vector<ConfirmCallback> takeAll() {
    std::vector<ConfirmCallback> out;
    out.reserve(pending_.size());
    for (auto& [_, e] : pending_) out.push_back(std::move(e.callback));
    pending_.clear();
    tagged_.clear();
    untagged_.clear();
    return out;
  }

  size_t outstanding() const { return pending_.size(); }

 private:
  struct Entry {
    ConfirmCallback callback;
    std::string messageId;
    std::string tag;
    bool mandatory = false;
    bool returned = false;
  };

  void unindex(uint64_t seq, const Entry& e) {
    if (!e.mandatory) return;
    if (!e.tag.empty()) {
      tagged_.erase(e.tag);
      return;
    }
    auto it = untagged_.find(e.messageId);
    if (it != untagged_.end() && it->second == seq) untagged_.erase(it);
  }

  std::string prefix_;
  uint64_t nextTag_ = 0;
  std::map<uint64_t, Entry> pending_;
  // At most one untagged outstanding mandatory publish per messageId: that is
  // what makes a return carrying only the messageId unambiguous.
  std::unordered_map<std::string, uint64_t> untagged_;
  std::unordered_map<std::string, uint64_t> tagged_;
};

}  // namespace protobus::amqp::detail
