// CancelListener: hears stream-cancellation notices and stops the matching
// in-flight stream in this process.
//
// Deliberately not a BaseListener. It needs its own channel and an unbounded
// prefetch: it must be heard while the service is busy inside a streaming
// handler, and request-serving machinery (retry, late ack, prefetch 1) would
// queue it behind exactly the work it is meant to interrupt. Its queue is
// exclusive and auto-delete, one per replica, and cancels are consumed
// without acknowledgement: a lost cancel means the stream runs on, the same
// outcome as never having sent one.
#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "protobus/connection.h"

namespace protobus {

class CancelListener : public std::enable_shared_from_this<CancelListener> {
 public:
  explicit CancelListener(std::shared_ptr<Connection> connection);
  ~CancelListener();

  // Best effort by design: a deployment whose credentials cannot declare the
  // cancel exchange keeps working without cancellation rather than failing to
  // start.
  void start();
  void close();

 private:
  void startInner();
  void restore();

  std::shared_ptr<Connection> connection_;
  std::mutex mutex_;
  std::shared_ptr<amqp::Channel> channel_;
  std::string queueName_;
  std::string consumerTag_;
  bool started_ = false;
  std::function<void()> detachRestorer_;
};

}  // namespace protobus
