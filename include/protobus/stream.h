// The caller's side of a server stream.
//
// ChunkStream yields the raw reply bodies of one streaming call; Stream<T>
// decodes them. Both block in next() until a chunk arrives, the stream ends
// (nullopt) or it fails (an exception: the service's error as a RemoteError,
// or a StreamTimeoutError, StreamBackpressureError, StreamSequenceError or
// DisconnectedError). A failure detected on this side (timeout, backpressure,
// a lost chunk) also tells the producer to stop, once.
//
//   for (const auto& tick : proxy.tick(request)) { ... }
//
// Leaving the loop early, calling cancel() or destroying the stream cancels
// it: the producer's signal fires and the framework stops publishing what it
// yields. Cancellation is best effort and cooperative.
#pragma once

#include <exception>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <string>

namespace protobus {

namespace detail {
struct StreamCall;
}

class ChunkStream {
 public:
  ChunkStream() = default;
  explicit ChunkStream(std::shared_ptr<detail::StreamCall> call);
  ChunkStream(ChunkStream&&) noexcept = default;
  ChunkStream& operator=(ChunkStream&& other) noexcept;
  ChunkStream(const ChunkStream&) = delete;
  ChunkStream& operator=(const ChunkStream&) = delete;
  ~ChunkStream();

  std::optional<std::string> next();
  void cancel();
  // True once the stream has ended, failed or been cancelled.
  bool finished() const;

 private:
  std::shared_ptr<detail::StreamCall> call_;
};

template <typename T>
class Stream {
 public:
  // Turns one raw chunk into a value; nullopt skips the chunk.
  using Decoder = std::function<std::optional<T>(const std::string& chunk)>;

  Stream() = default;
  Stream(ChunkStream chunks, Decoder decode) : chunks_(std::move(chunks)), decode_(std::move(decode)) {}

  // A stream whose first next() throws `error`.
  static Stream failed(std::exception_ptr error) {
    Stream s;
    s.error_ = std::move(error);
    return s;
  }

  std::optional<T> next() {
    if (error_) std::rethrow_exception(error_);
    while (auto chunk = chunks_.next()) {
      if (auto value = decode_(*chunk)) return value;
    }
    return std::nullopt;
  }

  void cancel() { chunks_.cancel(); }

  class iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;

    iterator() = default;
    explicit iterator(Stream* s) : stream_(s) { advance(); }
    const T& operator*() const { return *value_; }
    T& operator*() { return *value_; }
    const T* operator->() const { return &*value_; }
    iterator& operator++() {
      advance();
      return *this;
    }
    void operator++(int) { advance(); }
    friend bool operator==(const iterator& a, std::default_sentinel_t) { return !a.value_.has_value(); }

   private:
    void advance() { value_ = stream_->next(); }
    Stream* stream_ = nullptr;
    std::optional<T> value_;
  };

  iterator begin() { return iterator(this); }
  std::default_sentinel_t end() { return {}; }

 private:
  ChunkStream chunks_;
  Decoder decode_;
  std::exception_ptr error_;
};

}  // namespace protobus
