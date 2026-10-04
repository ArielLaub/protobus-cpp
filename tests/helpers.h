// Shared fixtures for the suites that run services on the in-memory broker.
#pragma once

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "pbtest.protobus.h"
#include "protobus/protobus.h"
#include "protobus/testing/memory_broker.h"

namespace pbtesting {

// Sets environment variables for the life of a test, restoring them after.
class ScopedEnv {
 public:
  ScopedEnv() = default;
  ScopedEnv(std::initializer_list<std::pair<const char*, const char*>> vars) {
    for (const auto& [k, v] : vars) set(k, v);
  }
  ~ScopedEnv() {
    for (auto& [k, old] : saved_) {
      if (old) {
        ::setenv(k.c_str(), old->c_str(), 1);
      } else {
        ::unsetenv(k.c_str());
      }
    }
  }
  void set(const char* name, const std::string& value) {
    if (!saved_.count(name)) {
      const char* old = std::getenv(name);
      saved_[name] = old ? std::optional<std::string>(old) : std::nullopt;
    }
    ::setenv(name, value.c_str(), 1);
  }

 private:
  std::map<std::string, std::optional<std::string>> saved_;
};

// Polls `predicate` until it holds or `timeout` passes.
template <typename P>
bool eventually(P predicate, std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!predicate()) {
    if (std::chrono::steady_clock::now() >= deadline) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  return true;
}

// A service implementing every rpc of pbtest.Calc, recording what it saw.
class CalcService : public pbtest::CalcBase {
 public:
  using CalcBase::CalcBase;

  std::atomic<int> failAttempts{0};
  std::atomic<int> slowStarted{0};
  std::atomic<int> slowAborted{0};
  std::atomic<int> yielded{0};
  std::atomic<bool> stoppedEarly{false};
  std::atomic<bool> finished{false};
  std::atomic<bool> cleanedUp{false};
  std::mutex mutex;
  std::vector<std::string> failMessageIds;

  pbtest::AddResponse add(const pbtest::AddRequest& r, protobus::CallContext&) override {
    pbtest::AddResponse out;
    out.set_result(r.a() + r.b());
    return out;
  }

  pbtest::DivideResponse divide(const pbtest::DivideRequest& r, protobus::CallContext&) override {
    if (r.divisor() == 0) throw protobus::HandledError("cannot divide by zero", "DIVISION_BY_ZERO");
    pbtest::DivideResponse out;
    out.set_quotient(r.dividend() / r.divisor());
    return out;
  }

  pbtest::Nothing fail(const pbtest::FailRequest& r, protobus::CallContext& ctx) override {
    ++failAttempts;
    {
      std::lock_guard<std::mutex> lock(mutex);
      failMessageIds.push_back(ctx.messageId);
    }
    if (r.handled()) throw protobus::HandledError("refused " + r.id(), "REFUSED");
    throw std::runtime_error("boom " + r.id());
  }

  pbtest::Nothing slow(const pbtest::SlowRequest& r, protobus::CallContext& ctx) override {
    ++slowStarted;
    if (ctx.signal.waitFor(std::chrono::milliseconds(r.ms()))) ++slowAborted;
    return {};
  }

  protobus::Generator<pbtest::Tick> ticks(const pbtest::TickRequest& r, protobus::CallContext& ctx) override {
    yielded = 0;
    stoppedEarly = false;
    finished = false;
    for (int i = 0; i < r.count(); ++i) {
      if (r.fail_at() > 0 && i >= r.fail_at()) {
        if (r.unhandled()) throw std::runtime_error("stream broke");
        throw protobus::HandledError("deliberate failure at chunk " + std::to_string(i), "TEST_FAIL");
      }
      if (ctx.signal.aborted()) {
        stoppedEarly = true;
        co_return;
      }
      pbtest::Tick t;
      t.set_seq(i);
      t.set_payload("chunk-" + std::to_string(i));
      co_yield t;
      ++yielded;
      if (r.delay_ms() > 0 && ctx.signal.waitFor(std::chrono::milliseconds(r.delay_ms()))) {
        stoppedEarly = true;
        co_return;
      }
    }
    finished = true;
  }

  pbtest::Who whoami(const pbtest::Nothing&, protobus::CallContext& ctx) override {
    pbtest::Who who;
    who.set_actor(ctx.actor);
    who.set_message_id(ctx.messageId);
    who.set_routing_key(ctx.routingKey);
    who.set_redelivered(ctx.redelivered);
    return who;
  }

  pbtest::Wallet echo(const pbtest::Wallet& w, protobus::CallContext&) override { return w; }

  void cleanup() override { cleanedUp = true; }
};

// The same service under an instance name.
class InstanceCalc : public CalcService {
 public:
  InstanceCalc(protobus::Context& ctx, std::string name, protobus::MessageServiceOptions o = {})
      : CalcService(ctx, std::move(o)), name_(std::move(name)) {}
  std::string ServiceName() const override { return name_; }

 private:
  std::string name_;
};

// A bus on the in-memory broker: a broker, a context and helpers to run
// services and proxies on it.
class MemoryBus : public ::testing::Test {
 protected:
  void SetUp() override {
    env.set("RPC_CALL_TIMEOUT_MS", "10000");
    env.set("STREAM_IDLE_TIMEOUT_MS", "10000");
    env.set("PUBLISH_CONFIRM_TIMEOUT_MS", "5000");
    if (!std::getenv("PROTOBUS_TEST_LOG")) protobus::setLogLevel(protobus::LogLevel::Silent);
    broker = protobus::testing::MemoryBroker::create();
    ctx = newContext();
  }

  void TearDown() override {
    services.clear();
    contexts.clear();
    ctx.reset();
    protobus::setLogLevel(protobus::LogLevel::Info);
  }

  std::unique_ptr<protobus::Context> newContext(protobus::ContextOptions options = fastReconnect()) {
    auto c = std::make_unique<protobus::Context>(broker);
    c->init("amqp://guest:guest@memory/", {}, options);
    return c;
  }

  static protobus::ContextOptions fastReconnect() {
    protobus::ContextOptions o;
    o.reconnection.initialDelayMs = 10;
    o.reconnection.maxDelayMs = 50;
    o.reconnection.maxRetries = 50;
    return o;
  }

  template <typename T = CalcService>
  std::shared_ptr<T> serve(protobus::MessageServiceOptions options = {}, protobus::Context* on = nullptr) {
    auto s = std::make_shared<T>(on ? *on : *ctx, options);
    s->init();
    services.push_back(s);
    return s;
  }

  pbtest::CalcProxy proxy(const std::string& name = pbtest::CalcProxy::kServiceName) {
    pbtest::CalcProxy p(*ctx, name);
    p.init();
    return p;
  }

  pbtesting::ScopedEnv env;
  std::shared_ptr<protobus::testing::MemoryBroker> broker;
  std::unique_ptr<protobus::Context> ctx;
  std::vector<std::shared_ptr<protobus::MessageService>> services;
  std::vector<std::unique_ptr<protobus::Context>> contexts;
};

// The value of a header as text, or "" when absent.
inline std::string header(const protobus::amqp::Delivery& d, const std::string& name) {
  if (!d.properties.headers) return "";
  auto it = d.properties.headers->find(name);
  if (it == d.properties.headers->end()) return "";
  return it->second.asString().value_or("");
}

}  // namespace pbtesting
