#include "protobus/runnable_service.h"

#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <vector>

#include "protobus/config.h"

namespace protobus {

namespace {

struct Entry {
  Context* context;
  std::shared_ptr<RunnableService> service;
};

struct Lifecycle {
  std::mutex mutex;
  std::condition_variable cv;
  std::vector<Entry> entries;
  bool shuttingDown = false;
  bool done = false;
  int exitCode = 0;
  bool exitAfterShutdown = true;
};

Lifecycle& lifecycle() {
  static auto* l = new Lifecycle();  // never destroyed: signal threads outlive main
  return *l;
}

int signalPipe[2] = {-1, -1};

extern "C" void onSignal(int sig) {
  const unsigned char b = static_cast<unsigned char>(sig);
  [[maybe_unused]] auto n = ::write(signalPipe[1], &b, 1);
}

void shutdown(const std::string& reason, int exitCode) {
  std::vector<Entry> entries;
  {
    std::lock_guard<std::mutex> lock(lifecycle().mutex);
    if (lifecycle().shuttingDown) return;
    lifecycle().shuttingDown = true;
    entries = lifecycle().entries;
  }
  Logger::info("Shutdown initiated" + (reason.empty() ? std::string() : " (signal: " + reason + ")"));

  // 1. Stop taking new work, keeping channels open. Cleanup must not run
  //    while consumers still deliver, or a request arrives after the
  //    service has released what it uses.
  for (auto& e : entries) {
    try {
      e.service->stopConsuming();
      Logger::info("Stopped accepting new messages");
    } catch (const std::exception& err) {
      Logger::error(std::string("Failed to stop consumers: ") + err.what());
    }
  }

  // 2. Let work in hand finish, including the reply, retry or DLQ publish
  //    that settles it.
  const int64_t budget = Config::shutdownDrainTimeoutMs();
  std::vector<Context*> contexts;
  for (auto& e : entries) {
    if (std::find(contexts.begin(), contexts.end(), e.context) == contexts.end()) contexts.push_back(e.context);
  }
  for (auto* ctx : contexts) {
    try {
      const size_t inFlight = ctx->connection().inFlightDeliveries();
      if (inFlight > 0) {
        Logger::info("Draining " + std::to_string(inFlight) + " in-flight message(s), up to " +
                     std::to_string(budget) + "ms");
        const bool drained = ctx->connection().drainInFlight(budget);
        Logger::info(drained ? std::string("In-flight messages drained")
                             : "Drain deadline reached with " +
                                   std::to_string(ctx->connection().inFlightDeliveries()) +
                                   " still running; they stay unacknowledged and will be redelivered");
      }
    } catch (const std::exception& err) {
      Logger::error(std::string("Drain failed: ") + err.what());
    }
  }

  // 3. Only now is it safe to release the services' resources.
  for (auto& e : entries) {
    try {
      e.service->cleanup();
      Logger::info("Service cleanup completed");
    } catch (const std::exception& err) {
      Logger::error(std::string("Service cleanup failed: ") + err.what());
    }
  }
  for (auto* ctx : contexts) {
    try {
      ctx->close();
      Logger::info("Connection closed");
    } catch (const std::exception& err) {
      Logger::error(std::string("Connection close failed: ") + err.what());
    }
  }

  bool exitAfter;
  {
    std::lock_guard<std::mutex> lock(lifecycle().mutex);
    lifecycle().done = true;
    lifecycle().exitCode = exitCode;
    exitAfter = lifecycle().exitAfterShutdown;
  }
  lifecycle().cv.notify_all();

  // 4. A bounded backstop: the process leaves even if something keeps it
  //    alive past the grace period.
  if (exitAfter) {
    const int64_t grace = Config::shutdownExitGraceMs();
    std::thread([grace, exitCode] {
      std::this_thread::sleep_for(std::chrono::milliseconds(grace));
      Logger::warn("Process still running " + std::to_string(grace) + "ms after shutdown; forcing exit");
      std::_Exit(exitCode);
    }).detach();
  }
}

}  // namespace

std::string RunnableService::ProtoFileName() const {
  const std::string name = ServiceName();
  const auto dot = name.find('.');
  return (dot == std::string::npos ? name : name.substr(0, dot)) + ".proto";
}

void RunnableService::installSignalHandlers() {
  static std::once_flag once;
  std::call_once(once, [] {
    if (::pipe(signalPipe) != 0) {
      Logger::error("RunnableService: cannot create the signal pipe; signals will not shut services down");
      return;
    }
    struct sigaction sa {};
    sa.sa_handler = onSignal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    std::thread([] {
      for (;;) {
        unsigned char sig = 0;
        const auto n = ::read(signalPipe[0], &sig, 1);
        if (n == 1) {
          shutdown(sig == SIGINT ? "SIGINT" : sig == SIGTERM ? "SIGTERM" : "signal", 0);
        } else if (n < 0 && errno == EINTR) {
          continue;
        } else {
          return;
        }
      }
    }).detach();
  });
}

void RunnableService::registerForShutdown(Context& context, std::shared_ptr<RunnableService> service) {
  std::lock_guard<std::mutex> lock(lifecycle().mutex);
  lifecycle().entries.push_back(Entry{&context, std::move(service)});
}

void RunnableService::abandonStart(Context& context, std::shared_ptr<RunnableService> service) {
  if (service) {
    try {
      service->stopConsuming();
    } catch (...) {
    }
    try {
      service->cleanup();
    } catch (...) {
    }
  }
  try {
    context.close();
  } catch (...) {
  }
}

int RunnableService::wait() {
  std::unique_lock<std::mutex> lock(lifecycle().mutex);
  lifecycle().cv.wait(lock, [] { return lifecycle().done; });
  return lifecycle().exitCode;
}

void RunnableService::requestShutdown(const std::string& reason) {
  std::thread([reason] { shutdown(reason, 0); }).detach();
}

void RunnableService::setExitAfterShutdown(bool exit) {
  std::lock_guard<std::mutex> lock(lifecycle().mutex);
  lifecycle().exitAfterShutdown = exit;
}

}  // namespace protobus
