// The C++ participant of the cross-language suite.
//
//   cpppeer server   serve the interop services; prints READY
//   cpppeer client   run the client scenario against PEER_TARGET's services;
//                    prints PASS/FAIL lines, then DONE
//
// The broker is PROTOBUS_TEST_AMQP. Behaviour mirrors the Go, TypeScript and
// Python peers exactly: the same services, the same canonical values, the
// same fifteen client checks.
#include <google/protobuf/util/message_differencer.h>
#include <signal.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>

#include "interop.protobus.h"
#include "protobus/protobus.h"

namespace {

const char* kLang = "cpp";

std::string env(const char* name, const char* fallback = "") {
  const char* v = std::getenv(name);
  return v && *v ? v : fallback;
}

protobus::Uint256 pow(int base, int exp) {
  protobus::Uint256 v(1);
  for (int i = 0; i < exp; ++i) v = v * protobus::Uint256(static_cast<uint64_t>(base));
  return v;
}

protobus::Uint256 twoTo70() { return pow(2, 70); }

// The Balance every peer returns for an ordinary account.
interop::Balance canonical() {
  interop::Balance b;
  *b.mutable_amount() = protobus::makeBigint(pow(10, 30));
  *b.mutable_as_of() = protobus::makeTimestampMs(1577836800000);  // 2020-01-01T00:00:00Z
  b.set_big(9007199254740993);
  b.add_tags("a");
  b.add_tags("b");
  (*b.mutable_counts())["x"] = 1;
  (*b.mutable_counts())["y"] = 2;
  (*b.mutable_balances())["k"] = protobus::makeBigint(pow(2, 200));
  for (uint64_t i = 1; i <= 3; ++i) *b.add_parts() = protobus::makeBigint(protobus::Uint256(i));
  b.set_kind(interop::KIND_FUTURE);
  auto* inner = b.mutable_inner();
  inner->set_name("root");
  *inner->mutable_value() = protobus::makeBigint(protobus::Uint256(7));
  auto* leaf = inner->add_children();
  leaf->set_name("leaf");
  *leaf->mutable_value() = protobus::makeBigint(protobus::Uint256(8));
  b.set_ubig(18446744073709551615ULL);
  b.set_blob(std::string("\x00\x01\xff", 3));
  b.set_ratio(0.5);
  b.set_flag(true);
  b.set_neg(-5);
  *b.mutable_before_epoch() = protobus::makeTimestampMs(-14182940000);  // 1969-07-20T20:17:40Z
  b.set_zero(0);
  return b;
}

// ---- server ----------------------------------------------------------------------

struct Produced {
  std::mutex mutex;
  interop::Produced value;
};

Produced& producedState() {
  static Produced p;
  return p;
}

class Counter : public interop::CounterBase {
 public:
  Counter(protobus::Context& ctx, std::string name) : CounterBase(ctx), name_(std::move(name)) {}
  std::string ServiceName() const override { return name_; }

  interop::AddResponse add(const interop::AddRequest& r, protobus::CallContext&) override {
    interop::AddResponse out;
    out.set_sum(r.a() + r.b());
    return out;
  }

  protobus::Generator<interop::Tick> tick(const interop::TickRequest& r, protobus::CallContext& ctx) override {
    if (r.emit_nothing()) co_return;
    auto record = [](const std::function<void(interop::Produced&)>& f) {
      std::lock_guard<std::mutex> lock(producedState().mutex);
      f(producedState().value);
    };
    record([](interop::Produced& p) { p.Clear(); });
    for (int i = 0; i < r.count(); ++i) {
      if (r.fail_at() > 0 && i >= r.fail_at()) {
        if (r.unhandled()) throw std::runtime_error("stream broke");
        throw protobus::HandledError("deliberate failure at chunk " + std::to_string(i), "TEST_FAIL");
      }
      if (ctx.signal.aborted()) {
        record([](interop::Produced& p) { p.set_stopped_early(true); });
        co_return;
      }
      interop::Tick t;
      t.set_seq(i);
      t.set_payload("chunk-" + std::to_string(i));
      co_yield t;
      record([](interop::Produced& p) { p.set_yielded(p.yielded() + 1); });
      if (r.delay_ms() > 0 && ctx.signal.waitFor(std::chrono::milliseconds(r.delay_ms()))) {
        record([](interop::Produced& p) { p.set_stopped_early(true); });
        co_return;
      }
    }
    record([](interop::Produced& p) { p.set_finished(true); });
  }

  interop::Produced produced(const interop::Nothing&, protobus::CallContext&) override {
    std::lock_guard<std::mutex> lock(producedState().mutex);
    return producedState().value;
  }

  interop::Who whoami(const interop::Nothing&, protobus::CallContext& ctx) override {
    interop::Who who;
    who.set_actor(ctx.actor);
    who.set_message_id(ctx.messageId);
    who.set_routing_key(ctx.routingKey);
    who.set_lang(kLang);
    return who;
  }

 private:
  std::string name_;
};

class Wallet : public interop::WalletBase {
 public:
  using WalletBase::WalletBase;

  interop::Balance balance(const interop::Query& q, protobus::CallContext&) override {
    if (q.account() == "boom") throw protobus::HandledError("no such account", "NOT_FOUND");
    if (q.account() == "crash") throw std::runtime_error("kaboom");
    return canonical();
  }

  interop::Balance echo(const interop::Balance& b, protobus::CallContext&) override { return b; }
};

class Flaky : public interop::FlakyBase {
 public:
  using FlakyBase::FlakyBase;

  interop::Nothing fail(const interop::FailRequest&, protobus::CallContext& ctx) override {
    interop::Attempted a;
    a.set_lang(kLang);
    a.set_message_id(ctx.messageId);
    context().publishEvent(a, "EVENT.attempted");
    throw std::runtime_error(std::string("flaky ") + kLang);
  }
};

class Listener : public interop::ListenerBase {
 public:
  using ListenerBase::ListenerBase;
  std::string ServiceName() const override { return std::string("interop.Listener.") + kLang; }
};

int serve() {
  protobus::Context ctx;
  ctx.init(env("PROTOBUS_TEST_AMQP"));

  protobus::MessageServiceOptions noRetry;
  noRetry.retry.maxRetries = 0;
  // The queues are shared with the other languages' replicas, so the retry
  // arguments must be equivalent to theirs.
  protobus::MessageServiceOptions flakyOptions;
  flakyOptions.retry.maxRetries = 3;
  flakyOptions.retry.retryDelayMs = 100;
  flakyOptions.maxConcurrent = 4;

  std::vector<std::shared_ptr<protobus::MessageService>> services = {
      std::make_shared<Counter>(ctx, "interop.Counter"),
      std::make_shared<Counter>(ctx, "interop.Counter.inst1"),
      std::make_shared<Wallet>(ctx, noRetry),
      std::make_shared<Flaky>(ctx, flakyOptions),
  };
  for (auto& s : services) s->init();
  auto listener = std::make_shared<Listener>(ctx, noRetry);
  listener->init();
  listener->subscribeEvent<interop::Ping>(
      [&ctx](const interop::Ping& ping, const std::string&, const std::string&) {
        interop::Ping pong;
        pong.set_id("pong:" + ping.id());
        *pong.mutable_n() = protobus::makeBigint(protobus::toUint256(ping.n()) + 1);
        pong.set_from(kLang);
        ctx.publishEvent(pong, std::string("EVENT.pong.") + kLang);
      },
      std::string("EVENT.ping.") + kLang);

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGTERM);
  sigaddset(&set, SIGINT);
  pthread_sigmask(SIG_BLOCK, &set, nullptr);
  std::cout << "READY" << std::endl;
  int sig = 0;
  sigwait(&set, &sig);
  for (auto& s : services) s->stopConsuming();
  listener->stopConsuming();
  ctx.connection().drainInFlight(5000);
  services.clear();
  listener.reset();
  ctx.close();
  return 0;
}

// ---- client ----------------------------------------------------------------------

int failures = 0;

void check(const std::string& name, const std::function<void()>& fn) {
  try {
    fn();
    std::cout << "PASS " << name << std::endl;
  } catch (const std::exception& e) {
    ++failures;
    std::string why = e.what();
    for (auto& c : why) {
      if (c == '\n') c = ' ';
    }
    std::cout << "FAIL " << name << ": " << why << std::endl;
  }
}

void require(bool ok, const std::string& what) {
  if (!ok) throw std::runtime_error(what);
}

template <typename T>
void eq(const T& got, const T& want, const std::string& what) {
  if (!(got == want)) {
    std::ostringstream m;
    m << what << ": got " << got << ", want " << want;
    throw std::runtime_error(m.str());
  }
}

void eqMessage(const google::protobuf::Message& got, const google::protobuf::Message& want, const std::string& what) {
  google::protobuf::util::MessageDifferencer diff;
  std::string report;
  diff.ReportDifferencesToString(&report);
  if (!diff.Compare(want, got)) throw std::runtime_error(what + " differs: " + report);
}

template <typename F>
protobus::RemoteError expectRemote(F&& f) {
  try {
    f();
  } catch (const protobus::RemoteError& e) {
    return e;
  }
  throw std::runtime_error("expected a RemoteError");
}

int client() {
  const std::string target = env("PEER_TARGET", kLang);
  protobus::Context ctx;
  ctx.init(env("PROTOBUS_TEST_AMQP"));
  interop::CounterProxy counter(ctx);
  counter.init();
  interop::CounterProxy inst(ctx, "interop.Counter.inst1");
  inst.init();
  interop::WalletProxy wallet(ctx);
  wallet.init();

  check("unary", [&] {
    interop::AddRequest r;
    r.set_a(2);
    r.set_b(3);
    eq(counter.add(r).sum(), 5, "sum");
  });
  check("priority on a plain queue", [&] {
    interop::AddRequest r;
    r.set_a(1);
    r.set_b(1);
    protobus::CallOptions o;
    o.priority = 2;
    eq(counter.add(r, o).sum(), 2, "sum");
  });
  check("stream in order", [&] {
    interop::TickRequest r;
    r.set_count(5);
    int i = 0;
    for (const auto& t : counter.tick(r)) {
      eq(t.seq(), i, "seq");
      eq(t.payload(), "chunk-" + std::to_string(i), "payload");
      ++i;
    }
    eq(i, 5, "chunks");
  });
  check("empty stream", [&] {
    interop::TickRequest r;
    r.set_emit_nothing(true);
    int n = 0;
    for (const auto& t : counter.tick(r)) {
      (void)t;
      ++n;
    }
    eq(n, 0, "chunks");
  });
  check("mid-stream handled error", [&] {
    interop::TickRequest r;
    r.set_count(10);
    r.set_fail_at(2);
    int n = 0;
    auto e = expectRemote([&] {
      for (const auto& t : counter.tick(r)) {
        (void)t;
        ++n;
      }
    });
    eq(e.code(), std::string("TEST_FAIL"), "code");
    require(std::string(e.what()).find("deliberate failure at chunk 2") != std::string::npos, e.what());
    eq(n, 2, "chunks before the error");
  });
  check("mid-stream unhandled error", [&] {
    interop::TickRequest r;
    r.set_count(10);
    r.set_fail_at(1);
    r.set_unhandled(true);
    auto e = expectRemote([&] {
      for (const auto& t : counter.tick(r)) (void)t;
    });
    eq(std::string(e.what()), std::string("stream broke"), "message");
  });
  check("cancellation reaches the producer", [&] {
    interop::TickRequest r;
    r.set_count(500);
    r.set_delay_ms(10);
    int n = 0;
    {
      auto stream = counter.tick(r);
      for (const auto& t : stream) {
        (void)t;
        if (++n == 3) break;
      }
    }
    interop::Produced p;
    for (int i = 0; i < 100; ++i) {
      p = counter.produced({});
      if (p.stopped_early()) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    require(p.stopped_early() && !p.finished() && p.yielded() < 500,
            "the " + target + " producer never saw the cancellation: " + p.ShortDebugString());
  });
  check("custom types, defaults and maps", [&] {
    interop::Query q;
    q.set_account("acc");
    eqMessage(wallet.balance(q), canonical(), "balance from " + target);
  });
  check("echo round trip", [&] { eqMessage(wallet.echo(canonical()), canonical(), "echo through " + target); });
  check("handled error", [&] {
    interop::Query q;
    q.set_account("boom");
    auto e = expectRemote([&] { wallet.balance(q); });
    eq(e.code(), std::string("NOT_FOUND"), "code");
    eq(std::string(e.what()), std::string("no such account"), "message");
  });
  check("unhandled error", [&] {
    interop::Query q;
    q.set_account("crash");
    auto e = expectRemote([&] { wallet.balance(q); });
    eq(std::string(e.what()), std::string("kaboom"), "message");
  });
  check("unimplemented method", [&] {
    auto e = expectRemote([&] { counter.unimplemented({}); });
    eq(e.code(), std::string("PROTOCOL_ERROR"), "code");
  });
  check("call metadata", [&] {
    protobus::CallOptions o;
    o.actor = "client-cpp";
    o.messageId = "mid-cpp-1";
    interop::Who want;
    want.set_actor("client-cpp");
    want.set_message_id("mid-cpp-1");
    want.set_routing_key("REQUEST.interop.Counter.whoami");
    want.set_lang(target);
    eqMessage(counter.whoami({}, o), want, "who");
  });
  check("instance routing", [&] {
    eq(inst.whoami({}).routing_key(), std::string("REQUEST.interop.Counter.inst1.whoami"), "routing key");
  });
  check("events both ways", [&] {
    auto listener = std::make_shared<protobus::EventListener>(ctx.connectionPtr(), ctx.factoryPtr());
    listener->init(nullptr, "");
    std::promise<interop::Ping> got;
    std::atomic<bool> once{false};
    listener->subscribe<interop::Ping>(
        [&](const interop::Ping& p, const std::string&, const std::string&) {
          if (!once.exchange(true)) got.set_value(p);
        },
        "EVENT.pong." + target);
    listener->start();
    interop::Ping ping;
    ping.set_id("cpp-1");
    *ping.mutable_n() = protobus::makeBigint(twoTo70());
    ping.set_from(kLang);
    ctx.publishEvent(ping, "EVENT.ping." + target);
    auto f = got.get_future();
    require(f.wait_for(std::chrono::seconds(10)) == std::future_status::ready, "no pong");
    auto pong = f.get();
    eq(pong.id(), std::string("pong:cpp-1"), "id");
    eq(pong.from(), target, "from");
    eq(protobus::toUint256(pong.n()).toString(), (twoTo70() + 1).toString(), "n");
    listener->close();
  });

  std::cout << "DONE" << std::endl;
  ctx.close();
  return failures == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
  protobus::setLogLevel(std::getenv("PROTOBUS_TEST_LOG") ? protobus::LogLevel::Debug : protobus::LogLevel::Error);
  const std::string mode = argc > 1 ? argv[1] : "client";
  try {
    return mode == "server" ? serve() : client();
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    return 2;
  }
}
