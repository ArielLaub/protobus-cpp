#include "protobus/abort.h"

#include <condition_variable>
#include <map>
#include <mutex>
#include <vector>

#include "protobus/errors.h"

namespace protobus {

namespace detail {
struct AbortState {
  std::mutex mutex;
  std::condition_variable cv;
  bool aborted = false;
  uint64_t nextId = 1;
  std::map<uint64_t, std::function<void()>> listeners;
};
}  // namespace detail

AbortSignal::AbortSignal() : state_(std::make_shared<detail::AbortState>()) {}

AbortSignal::AbortSignal(std::shared_ptr<detail::AbortState> state) : state_(std::move(state)) {}

bool AbortSignal::aborted() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->aborted;
}

uint64_t AbortSignal::addListener(std::function<void()> fn) const {
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (!state_->aborted) {
      const uint64_t id = state_->nextId++;
      state_->listeners.emplace(id, std::move(fn));
      return id;
    }
  }
  fn();
  return 0;
}

void AbortSignal::removeListener(uint64_t id) const {
  if (id == 0) return;
  std::lock_guard<std::mutex> lock(state_->mutex);
  state_->listeners.erase(id);
}

bool AbortSignal::waitFor(std::chrono::milliseconds timeout) const {
  std::unique_lock<std::mutex> lock(state_->mutex);
  state_->cv.wait_for(lock, timeout, [&] { return state_->aborted; });
  return state_->aborted;
}

void AbortSignal::throwIfAborted() const {
  if (aborted()) throw Error("the operation was aborted", "AbortError");
}

AbortController::AbortController() : state_(std::make_shared<detail::AbortState>()) {}

AbortSignal AbortController::signal() const { return AbortSignal(state_); }

void AbortController::abort() {
  std::map<uint64_t, std::function<void()>> listeners;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->aborted) return;
    state_->aborted = true;
    listeners.swap(state_->listeners);
  }
  state_->cv.notify_all();
  for (auto& [_, fn] : listeners) {
    try {
      fn();
    } catch (...) {
    }
  }
}

}  // namespace protobus
