// The cross-language suite: protobus-cpp against the TypeScript, Python and
// Go ports' real libraries over a real broker, in both directions.
//
//   - the C++ client against a C++, TypeScript, Python and Go server;
//   - the TypeScript, Python and Go clients against a C++ server;
//   - replicas of one service in all four languages sharing a queue and its
//     retry ladder.
//
// It needs the broker of tests/integration (PROTOBUS_TEST_AMQP_URL and
// PROTOBUS_TEST_MGMT_URL) and the sibling checkouts: PROTOBUS_TS (default
// ../protobus, built with `npm run build-ts`), PROTOBUS_PY (default
// ../protobus-py, with a venv/) and PROTOBUS_GO (default ../protobus-go, with
// a Go toolchain on the PATH). A missing peer skips its tests, unless its
// variable is set explicitly (as CI does): then its absence is a failure.
#include <fcntl.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <future>
#include <map>
#include <set>
#include <sstream>

#include "broker.h"
#include "interop.protobus.h"
#include "protobus/protobus.h"

extern char** environ;

namespace {

namespace fs = std::filesystem;

fs::path here() { return fs::path(PROTOBUS_CROSSLANG_DIR); }

template <typename P>
bool eventually_(P predicate, std::chrono::milliseconds timeout = std::chrono::seconds(30)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!predicate()) {
    if (std::chrono::steady_clock::now() >= deadline) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return true;
}

fs::path sibling(const char* var, const char* name) {
  if (const char* v = std::getenv(var); v && *v) return v;
  return here().parent_path().parent_path() / name;
}

bool configured(const char* var) {
  const char* v = std::getenv(var);
  return v && *v;
}

// A peer process, its stdout read line by line.
class Process {
 public:
  Process(const std::vector<std::string>& argv, const std::map<std::string, std::string>& extraEnv) {
    int out[2];
    if (::pipe(out) != 0) throw std::runtime_error("pipe failed");
    stderrPath_ = (fs::temp_directory_path() / ("pbcpp-peer-" + std::to_string(::getpid()) + "-" +
                                                std::to_string(counter()++) + ".err"))
                      .string();
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_adddup2(&actions, out[1], 1);
    posix_spawn_file_actions_addclose(&actions, out[0]);
    posix_spawn_file_actions_addopen(&actions, 2, stderrPath_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);

    std::map<std::string, std::string> env;
    for (char** e = environ; *e; ++e) {
      std::string kv = *e;
      auto eq = kv.find('=');
      if (eq != std::string::npos) env[kv.substr(0, eq)] = kv.substr(eq + 1);
    }
    for (const auto& [k, v] : extraEnv) env[k] = v;
    std::vector<std::string> envStrings;
    for (const auto& [k, v] : env) envStrings.push_back(k + "=" + v);
    std::vector<char*> envp;
    for (auto& s : envStrings) envp.push_back(s.data());
    envp.push_back(nullptr);
    std::vector<char*> args;
    std::vector<std::string> copy = argv;
    for (auto& a : copy) args.push_back(a.data());
    args.push_back(nullptr);

    const int rc = posix_spawnp(&pid_, args[0], &actions, nullptr, args.data(), envp.data());
    posix_spawn_file_actions_destroy(&actions);
    ::close(out[1]);
    if (rc != 0) {
      ::close(out[0]);
      throw std::runtime_error("cannot start " + argv[0]);
    }
    in_ = ::fdopen(out[0], "r");
  }

  ~Process() {
    terminate();
    if (in_) ::fclose(in_);
    std::error_code ec;
    fs::remove(stderrPath_, ec);
  }

  // The next stdout line, or nullopt at end of output.
  std::optional<std::string> line() {
    char* buf = nullptr;
    size_t cap = 0;
    const ssize_t n = ::getline(&buf, &cap, in_);
    if (n < 0) {
      free(buf);
      return std::nullopt;
    }
    std::string s(buf, static_cast<size_t>(n));
    free(buf);
    while (!s.empty() && (s.back() == '\n' || s.back() == '\r')) s.pop_back();
    return s;
  }

  int wait() {
    if (pid_ <= 0) return exit_;
    int status = 0;
    ::waitpid(pid_, &status, 0);
    pid_ = -1;
    exit_ = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
    return exit_;
  }

  void terminate() {
    if (pid_ <= 0) return;
    ::kill(pid_, SIGTERM);
    for (int i = 0; i < 100; ++i) {
      int status = 0;
      if (::waitpid(pid_, &status, WNOHANG) == pid_) {
        pid_ = -1;
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ::kill(pid_, SIGKILL);
    ::waitpid(pid_, nullptr, 0);
    pid_ = -1;
  }

  std::string stderrText() const {
    std::ifstream f(stderrPath_);
    std::stringstream s;
    s << f.rdbuf();
    return s.str();
  }

 private:
  static int& counter() {
    static int c = 0;
    return c;
  }
  pid_t pid_ = -1;
  int exit_ = -1;
  FILE* in_ = nullptr;
  std::string stderrPath_;
};

struct Peer {
  std::string lang;
  const char* var;  // the variable that configures it, if any
  // The command for a mode, or an error saying why the peer is unavailable.
  std::function<std::vector<std::string>(const std::string& mode, std::string& why)> command;
  std::function<std::map<std::string, std::string>()> env;
};

std::string goPeerBinary(std::string& why) {
  static std::string built;
  static std::string failure;
  static std::once_flag once;
  std::call_once(once, [] {
    const fs::path go = sibling("PROTOBUS_GO", "protobus-go");
    if (!fs::exists(go / "crosslang" / "gopeer" / "gopeer.go")) {
      failure = "protobus-go not found at " + go.string() + " (set PROTOBUS_GO)";
      return;
    }
    // Build in a scratch copy whose go.mod points at the checkout under test.
    const fs::path dir = fs::temp_directory_path() / ("pbcpp-gopeer-" + std::to_string(::getpid()));
    fs::remove_all(dir);
    fs::create_directories(dir);
    fs::copy_file(here() / "peers" / "go" / "main.go", dir / "main.go");
    fs::copy_file(go / "go.sum", dir / "go.sum");
    {
      std::ofstream mod(dir / "go.mod");
      mod << "module protobus-cpp/crosslang/gopeer\n\ngo 1.25\n\nrequire github.com/ArielLaub/protobus-go/v2 v2.0.0\n\n"
          << "replace github.com/ArielLaub/protobus-go/v2 => " << fs::absolute(go).string() << "\n";
    }
    const std::string out = (dir / "gopeer").string();
    const std::string cmd = "cd '" + dir.string() + "' && GOFLAGS=-mod=mod go build -o '" + out + "' . 2>&1";
    if (std::system(cmd.c_str()) != 0 || !fs::exists(out)) {
      failure = "could not build the Go peer (is Go on the PATH?)";
      return;
    }
    built = out;
  });
  why = failure;
  return built;
}

std::vector<Peer> peers() {
  return {
      {"cpp", nullptr,
       [](const std::string& mode, std::string&) { return std::vector<std::string>{PROTOBUS_CPPPEER, mode}; },
       [] { return std::map<std::string, std::string>{}; }},
      {"ts", "PROTOBUS_TS",
       [](const std::string& mode, std::string& why) {
         const fs::path ts = sibling("PROTOBUS_TS", "protobus");
         if (!fs::exists(ts / "dist" / "lib" / "context.js")) {
           why = "TypeScript protobus not built at " + ts.string() + " (set PROTOBUS_TS)";
           return std::vector<std::string>{};
         }
         return std::vector<std::string>{"node", (here() / "peers" / "ts" / "peer.js").string(), mode};
       },
       [] { return std::map<std::string, std::string>{{"PROTOBUS_TS", sibling("PROTOBUS_TS", "protobus").string()}}; }},
      {"py", "PROTOBUS_PY",
       [](const std::string& mode, std::string& why) {
         const fs::path py = sibling("PROTOBUS_PY", "protobus-py");
         const fs::path interp = py / "venv" / "bin" / "python";
         if (!fs::exists(interp)) {
           why = "protobus-py venv not found at " + py.string() + " (set PROTOBUS_PY)";
           return std::vector<std::string>{};
         }
         return std::vector<std::string>{interp.string(), (here() / "peers" / "py" / "peer.py").string(), mode};
       },
       [] { return std::map<std::string, std::string>{{"PYTHONPATH", sibling("PROTOBUS_PY", "protobus-py").string()}}; }},
      {"go", "PROTOBUS_GO",
       [](const std::string& mode, std::string& why) {
         const std::string bin = goPeerBinary(why);
         if (bin.empty()) return std::vector<std::string>{};
         return std::vector<std::string>{bin, mode};
       },
       [] { return std::map<std::string, std::string>{}; }},
  };
}

const Peer& peer(const std::string& lang) {
  static const auto all = peers();
  for (const auto& p : all) {
    if (p.lang == lang) return p;
  }
  throw std::runtime_error("no peer " + lang);
}

class CrossLang : public ::testing::Test {
 protected:
  void SetUp() override {
    broker_ = integration::Broker::require();
    if (broker_ == nullptr) GTEST_SKIP() << "PROTOBUS_TEST_AMQP_URL and PROTOBUS_TEST_MGMT_URL are not both set";
    if (!std::getenv("PROTOBUS_TEST_LOG")) protobus::setLogLevel(protobus::LogLevel::Silent);
    vhost_ = broker_->newVHost();
  }

  void TearDown() override {
    servers_.clear();
    vhost_.reset();
  }

  // Skips the test when the peer is unavailable, unless it was configured
  // explicitly.
  std::vector<std::string> commandFor(const Peer& p, const std::string& mode) {
    std::string why;
    auto cmd = p.command(mode, why);
    if (cmd.empty()) {
      if (p.var && configured(p.var)) {
        ADD_FAILURE() << why;
      } else {
        unavailable_ = why;
      }
    }
    return cmd;
  }

  std::map<std::string, std::string> envFor(const Peer& p, const std::string& target) {
    auto env = p.env();
    env["PROTOBUS_TEST_AMQP"] = vhost_->url();
    env["PROTOBUS_TEST_PROTO_DIR"] = (here() / "proto").string();
    env["PEER_TARGET"] = target;
    return env;
  }

  // Start a peer's server, returning once it has printed READY.
  bool startServer(const std::string& lang) {
    const Peer& p = peer(lang);
    auto cmd = commandFor(p, "server");
    if (cmd.empty()) return false;
    auto proc = std::make_unique<Process>(cmd, envFor(p, lang));
    auto ready = std::async(std::launch::async, [&proc] {
      while (auto l = proc->line()) {
        if (*l == "READY") return true;
      }
      return false;
    });
    if (ready.wait_for(std::chrono::seconds(90)) != std::future_status::ready || !ready.get()) {
      ADD_FAILURE() << lang << " server did not become ready\n" << proc->stderrText();
      return false;
    }
    servers_.push_back(std::move(proc));
    return true;
  }

  // Run a peer's client scenario against `target`, one sub-result per check.
  void runClient(const std::string& lang, const std::string& target) {
    const Peer& p = peer(lang);
    auto cmd = commandFor(p, "client");
    if (cmd.empty()) return;
    Process proc(cmd, envFor(p, target));
    int passed = 0;
    bool done = false;
    std::vector<std::string> failed;
    while (auto l = proc.line()) {
      if (l->rfind("PASS ", 0) == 0) {
        ++passed;
      } else if (l->rfind("FAIL ", 0) == 0) {
        failed.push_back(l->substr(5));
      } else if (*l == "DONE") {
        done = true;
      }
    }
    const int code = proc.wait();
    for (const auto& f : failed) ADD_FAILURE() << lang << " client against " << target << ": " << f;
    EXPECT_TRUE(done) << lang << " client did not finish (exit " << code << ")\n" << proc.stderrText();
    EXPECT_GE(passed, 15) << lang << " client against " << target << " passed only " << passed << " checks";
  }

  integration::Broker* broker_ = nullptr;
  std::unique_ptr<integration::VHost> vhost_;
  std::vector<std::unique_ptr<Process>> servers_;
  std::string unavailable_;
};

#define SKIP_IF_UNAVAILABLE()                    \
  if (!unavailable_.empty()) GTEST_SKIP() << unavailable_

class CppClient : public CrossLang, public ::testing::WithParamInterface<std::string> {};

TEST_P(CppClient, AgainstEveryServer) {
  const std::string target = GetParam();
  if (!startServer(target)) {
    SKIP_IF_UNAVAILABLE();
    return;
  }
  runClient("cpp", target);
}

INSTANTIATE_TEST_SUITE_P(Servers, CppClient, ::testing::Values("cpp", "ts", "py", "go"),
                         [](const auto& info) { return info.param; });

class PeerClients : public CrossLang, public ::testing::WithParamInterface<std::string> {};

TEST_P(PeerClients, AgainstACppServer) {
  ASSERT_TRUE(startServer("cpp"));
  runClient(GetParam(), "cpp");
  SKIP_IF_UNAVAILABLE();
}

INSTANTIATE_TEST_SUITE_P(Clients, PeerClients, ::testing::Values("ts", "py", "go"),
                         [](const auto& info) { return info.param; });

// interop.Flaky in all four languages at once, competing on one queue. Every
// attempt fails, so each message climbs the retry ladder across replicas of
// different languages: the x-retry-count one writes is read by the others,
// the queue arguments each declares must be equivalent to the rest's, and the
// message must end in the dead-letter queue exactly once, after exactly three
// retries.
TEST_F(CrossLang, MixedReplicasShareOneRetryLadder) {
  for (const char* lang : {"cpp", "ts", "py", "go"}) {
    if (!startServer(lang)) {
      SKIP_IF_UNAVAILABLE();
      return;
    }
  }

  protobus::Context ctx;
  ctx.init(vhost_->url());
  auto listener = std::make_shared<protobus::EventListener>(ctx.connectionPtr(), ctx.factoryPtr());
  listener->init(nullptr, "");
  std::mutex m;
  std::map<std::string, int> perMessage;
  std::map<std::string, int> langs;
  int total = 0;
  listener->subscribe<interop::Attempted>(
      [&](const interop::Attempted& a, const std::string&, const std::string&) {
        std::lock_guard<std::mutex> lock(m);
        ++perMessage[a.message_id()];
        ++langs[a.lang()];
        ++total;
      },
      "EVENT.attempted");
  listener->start();

  constexpr int kMessages = 9;
  constexpr int kRetries = 3;
  interop::FlakyProxy flaky(ctx);
  flaky.init();
  std::vector<std::future<void>> calls;
  for (int i = 0; i < kMessages; ++i) {
    calls.push_back(std::async(std::launch::async, [&flaky, i] {
      interop::FailRequest r;
      r.set_id(std::to_string(i));
      protobus::CallOptions o;
      o.messageId = "flaky-" + std::to_string(i);
      o.timeoutMs = 30000;
      try {
        flaky.fail(r, o);
        ADD_FAILURE() << "message " << i << " succeeded";
      } catch (const protobus::RemoteError& e) {
        // The caller is answered with the final failure.
        EXPECT_EQ(std::string(e.what()).rfind("flaky ", 0), 0u) << e.what();
      }
    }));
  }
  for (auto& c : calls) c.get();

  ASSERT_TRUE(eventually_([&] {
    std::lock_guard<std::mutex> lock(m);
    return total >= kMessages * (kRetries + 1);
  })) << "saw " << total << " attempts";
  {
    std::lock_guard<std::mutex> lock(m);
    for (const auto& [id, n] : perMessage) EXPECT_EQ(n, kRetries + 1) << id;
    std::string seen;
    for (const auto& [lang, n] : langs) seen += lang + "=" + std::to_string(n) + " ";
    std::cout << "attempts by language: " << seen << std::endl;
    EXPECT_GE(langs.size(), 2u) << "the ladder never crossed languages; the test proves nothing";
  }

  ASSERT_EQ(vhost_->waitQueueDepth("interop.Flaky.DLQ", kMessages), kMessages);
  auto ch = ctx.connection().openChannel();
  std::vector<protobus::amqp::Delivery> dead;
  ch->consume("interop.Flaky.DLQ", "dlq", true, false,
              [&](protobus::amqp::Delivery d) {
                std::lock_guard<std::mutex> lock(m);
                dead.push_back(std::move(d));
              },
              nullptr);
  ASSERT_TRUE(eventually_([&] {
    std::lock_guard<std::mutex> lock(m);
    return dead.size() >= static_cast<size_t>(kMessages);
  }));
  std::set<std::string> ids;
  std::lock_guard<std::mutex> lock(m);
  for (const auto& d : dead) {
    const auto& h = *d.properties.headers;
    EXPECT_EQ(h.at("x-retry-count").asInt(), kRetries);
    EXPECT_EQ(h.at("x-original-queue").asString(), "interop.Flaky");
    EXPECT_EQ(h.at("x-original-routing-key").asString(), "REQUEST.interop.Flaky.fail");
    const std::string last = h.at("x-last-error").asString().value_or("");
    EXPECT_FALSE(last.empty());
    EXPECT_EQ(last.find("flaky"), std::string::npos) << "x-last-error must name the class, never the message: " << last;
    ids.insert(d.properties.messageId.value_or(""));
  }
  EXPECT_EQ(ids.size(), static_cast<size_t>(kMessages)) << "each message must be dead-lettered exactly once";
}

}  // namespace
