// AbortController and AbortSignal, after the web platform's.
//
// A service handler receives an AbortSignal that fires when its processing
// timeout elapses or, for a stream, when the caller cancels. A caller passes
// one in StreamOptions to cancel a stream from anywhere. Cancellation is
// cooperative: C++ cannot preempt a running function, so a handler doing long
// work should check the signal, or wait on it instead of sleeping.
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

namespace protobus {

namespace detail {
struct AbortState;
}

class AbortSignal {
 public:
  // A signal that never fires.
  AbortSignal();

  bool aborted() const;

  // Run `fn` once when the signal fires; at once if it already has. Returns
  // an id for removeListener. `fn` runs on whichever thread aborts (for a
  // handler's processing timeout or a cancelled stream, one of the
  // connection's workers), so keep it short.
  uint64_t addListener(std::function<void()> fn) const;
  void removeListener(uint64_t id) const;

  // Block until the signal fires or `timeout` elapses. Returns aborted().
  // Use it in place of sleep_for in a cancellable handler.
  bool waitFor(std::chrono::milliseconds timeout) const;

  // Throws protobus::Error("aborted") when the signal has fired.
  void throwIfAborted() const;

 private:
  friend class AbortController;
  explicit AbortSignal(std::shared_ptr<detail::AbortState> state);
  std::shared_ptr<detail::AbortState> state_;
};

class AbortController {
 public:
  AbortController();

  AbortSignal signal() const;
  // Fire the signal. Idempotent.
  void abort();

 private:
  std::shared_ptr<detail::AbortState> state_;
};

}  // namespace protobus
