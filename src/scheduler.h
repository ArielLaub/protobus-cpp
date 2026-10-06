// Timers: the C++ stand-in for setTimeout. One thread fires callbacks at
// their deadlines; a callback must be short and must not block, so anything
// substantial is handed to an Executor.
//
// The timer thread holds the scheduler's state itself, so the scheduler may
// be destroyed from inside one of its own callbacks.
#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>

namespace protobus::detail {

class Scheduler {
 public:
  using Clock = std::chrono::steady_clock;
  using TimerId = uint64_t;

  Scheduler();
  ~Scheduler();

  Scheduler(const Scheduler&) = delete;
  Scheduler& operator=(const Scheduler&) = delete;

  TimerId schedule(std::chrono::milliseconds delay, std::function<void()> fn);
  // True when the timer was removed before it fired.
  bool cancel(TimerId id);
  // Timers armed and not yet fired or cancelled.
  size_t pending() const;

 private:
  struct State {
    std::mutex mutex;
    std::condition_variable cv;
    // Ordered by deadline, then by id so equal deadlines fire in order.
    std::map<std::pair<Clock::time_point, TimerId>, std::function<void()>> timers;
    std::map<TimerId, Clock::time_point> deadlines;
    TimerId nextId = 1;
    bool stopping = false;
  };

  static void run(std::shared_ptr<State> state);

  std::shared_ptr<State> state_;
  std::thread thread_;
};

}  // namespace protobus::detail
