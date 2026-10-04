#include "scheduler.h"

#include "protobus/logger.h"

namespace protobus::detail {

Scheduler::Scheduler() : state_(std::make_shared<State>()) { thread_ = std::thread(&Scheduler::run, state_); }

Scheduler::~Scheduler() {
  std::map<std::pair<Clock::time_point, TimerId>, std::function<void()>> dropped;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->stopping = true;
    dropped.swap(state_->timers);
    state_->deadlines.clear();
  }
  state_->cv.notify_all();
  if (thread_.get_id() == std::this_thread::get_id()) {
    thread_.detach();
  } else if (thread_.joinable()) {
    thread_.join();
  }
  // Pending callbacks are released here, outside the lock: they may own
  // objects whose destructors take it.
}

Scheduler::TimerId Scheduler::schedule(std::chrono::milliseconds delay, std::function<void()> fn) {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->stopping) return 0;
  const TimerId id = state_->nextId++;
  const auto deadline = Clock::now() + delay;
  state_->timers.emplace(std::make_pair(deadline, id), std::move(fn));
  state_->deadlines.emplace(id, deadline);
  state_->cv.notify_all();
  return id;
}

bool Scheduler::cancel(TimerId id) {
  std::function<void()> dropped;
  std::lock_guard<std::mutex> lock(state_->mutex);
  auto it = state_->deadlines.find(id);
  if (it == state_->deadlines.end()) return false;
  auto timer = state_->timers.find(std::make_pair(it->second, id));
  if (timer != state_->timers.end()) {
    dropped = std::move(timer->second);
    state_->timers.erase(timer);
  }
  state_->deadlines.erase(it);
  return true;
}

void Scheduler::run(std::shared_ptr<State> state) {
  std::unique_lock<std::mutex> lock(state->mutex);
  while (!state->stopping) {
    if (state->timers.empty()) {
      state->cv.wait(lock);
      continue;
    }
    auto first = state->timers.begin();
    const auto deadline = first->first.first;
    if (Clock::now() < deadline) {
      state->cv.wait_until(lock, deadline);
      continue;
    }
    auto fn = std::move(first->second);
    state->deadlines.erase(first->first.second);
    state->timers.erase(first);
    lock.unlock();
    try {
      fn();
    } catch (const std::exception& e) {
      Logger::error(std::string("protobus: a timer callback threw: ") + e.what());
    } catch (...) {
      Logger::error("protobus: a timer callback threw a non-standard exception");
    }
    fn = nullptr;
    lock.lock();
  }
}

}  // namespace protobus::detail
