#include "executor.h"

#include <thread>

#include "protobus/logger.h"

namespace protobus::detail {

namespace {
// The pool the current thread works for, so a shutdown issued from inside a
// task does not wait for itself.
thread_local const void* currentPool = nullptr;
}  // namespace

Executor::Executor(std::chrono::milliseconds idleTimeout) : state_(std::make_shared<State>()) {
  state_->idleTimeout = idleTimeout;
}

Executor::~Executor() { shutdown(std::chrono::seconds(5)); }

void Executor::post(std::function<void()> task) {
  bool spawn = false;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->stopping) return;
    state_->tasks.push_back(std::move(task));
    if (state_->idle >= state_->tasks.size()) {
      state_->wake.notify_one();
    } else {
      ++state_->workers;
      spawn = true;
    }
  }
  if (spawn) std::thread(&Executor::work, state_).detach();
}

void Executor::work(std::shared_ptr<State> state) {
  currentPool = state.get();
  std::unique_lock<std::mutex> lock(state->mutex);
  for (;;) {
    if (state->tasks.empty()) {
      if (state->stopping) break;
      ++state->idle;
      const bool woke = state->wake.wait_for(lock, state->idleTimeout,
                                             [&] { return !state->tasks.empty() || state->stopping; });
      --state->idle;
      if (!woke) break;
      continue;
    }
    auto task = std::move(state->tasks.front());
    state->tasks.pop_front();
    lock.unlock();
    try {
      task();
    } catch (const std::exception& e) {
      Logger::error(std::string("protobus: a background task threw: ") + e.what());
    } catch (...) {
      Logger::error("protobus: a background task threw a non-standard exception");
    }
    task = nullptr;
    lock.lock();
  }
  --state->workers;
  state->exited.notify_all();
}

bool Executor::shutdown(std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(state_->mutex);
  state_->stopping = true;
  state_->wake.notify_all();
  const size_t self = currentPool == state_.get() ? 1 : 0;
  return state_->exited.wait_for(lock, timeout, [&] { return state_->workers <= self; });
}

}  // namespace protobus::detail
