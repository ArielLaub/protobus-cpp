// An elastic thread pool: a task runs on an idle worker if there is one and
// on a new worker otherwise, and workers idle for long enough exit.
//
// Elastic rather than fixed, because protobus handlers block: a handler that
// calls another service waits for a reply that is itself delivered through
// this pool, and a fixed pool saturated by such handlers would deadlock. The
// number of threads is bounded in practice by the consumers' prefetch.
#pragma once

#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>

namespace protobus::detail {

class Executor {
 public:
  explicit Executor(std::chrono::milliseconds idleTimeout = std::chrono::seconds(30));
  ~Executor();

  Executor(const Executor&) = delete;
  Executor& operator=(const Executor&) = delete;

  void post(std::function<void()> task);

  // Stop accepting tasks and wait up to `timeout` for every worker to exit.
  // Returns false when some task was still running at the deadline; its
  // worker is left to finish on its own.
  bool shutdown(std::chrono::milliseconds timeout);

 private:
  struct State {
    std::mutex mutex;
    std::condition_variable wake;
    std::condition_variable exited;
    std::deque<std::function<void()>> tasks;
    size_t idle = 0;
    size_t workers = 0;
    bool stopping = false;
    std::chrono::milliseconds idleTimeout;
  };

  static void work(std::shared_ptr<State> state);

  std::shared_ptr<State> state_;
};

}  // namespace protobus::detail
