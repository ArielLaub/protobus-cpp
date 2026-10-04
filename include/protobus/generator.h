// Generator<T>: a synchronous C++20 coroutine generator, the C++ counterpart
// of the async generator a TypeScript streaming handler returns.
//
//   protobus::Generator<Tick> tick(const TickRequest& request, protobus::CallContext& ctx) override {
//     for (int i = 0; i < request.count(); ++i) {
//       if (ctx.signal.aborted()) co_return;
//       Tick t;
//       t.set_seq(i);
//       co_yield t;
//     }
//   }
//
// The body does not start until the first chunk is pulled, and the framework
// pulls one chunk at a time, publishing each before asking for the next, so a
// producer is paced by the broker. When the caller cancels, the framework
// stops pulling and destroys the generator: the coroutine frame unwinds and
// the destructors of its locals run, which is how a producer releases what it
// holds. An exception thrown in the body ends the stream with an error the
// caller receives.
#pragma once

#include <coroutine>
#include <exception>
#include <iterator>
#include <optional>
#include <utility>

namespace protobus {

template <typename T>
class Generator {
 public:
  struct promise_type {
    std::optional<T> current;
    std::exception_ptr error;

    Generator get_return_object() { return Generator(std::coroutine_handle<promise_type>::from_promise(*this)); }
    std::suspend_always initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }

    template <typename U>
    std::suspend_always yield_value(U&& value) {
      current.emplace(std::forward<U>(value));
      return {};
    }

    void return_void() noexcept {}
    void unhandled_exception() { error = std::current_exception(); }
  };

  using handle_type = std::coroutine_handle<promise_type>;

  Generator() = default;
  explicit Generator(handle_type h) : handle_(h) {}
  Generator(Generator&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
  Generator& operator=(Generator&& other) noexcept {
    if (this != &other) {
      reset();
      handle_ = std::exchange(other.handle_, {});
    }
    return *this;
  }
  Generator(const Generator&) = delete;
  Generator& operator=(const Generator&) = delete;
  ~Generator() { reset(); }

  // Run the body to its next co_yield. Returns the yielded value, or nullopt
  // once the body has finished. Rethrows an exception the body threw.
  std::optional<T> next() {
    if (!handle_ || handle_.done()) return std::nullopt;
    handle_.promise().current.reset();
    handle_.resume();
    if (handle_.promise().error) {
      auto error = std::exchange(handle_.promise().error, nullptr);
      std::rethrow_exception(error);
    }
    if (handle_.done()) return std::nullopt;
    return std::move(handle_.promise().current);
  }

  bool valid() const { return static_cast<bool>(handle_); }

  // Destroy the coroutine frame now, unwinding its locals.
  void reset() {
    if (handle_) {
      handle_.destroy();
      handle_ = {};
    }
  }

  // Range-for support, so a generator can also be consumed directly.
  class iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;

    iterator() = default;
    explicit iterator(Generator* g) : gen_(g) { advance(); }
    const T& operator*() const { return *value_; }
    T& operator*() { return *value_; }
    iterator& operator++() {
      advance();
      return *this;
    }
    void operator++(int) { advance(); }
    friend bool operator==(const iterator& a, std::default_sentinel_t) { return !a.value_.has_value(); }

   private:
    void advance() { value_ = gen_->next(); }
    Generator* gen_ = nullptr;
    std::optional<T> value_;
  };

  iterator begin() { return iterator(this); }
  std::default_sentinel_t end() { return {}; }

 private:
  handle_type handle_;
};

}  // namespace protobus
