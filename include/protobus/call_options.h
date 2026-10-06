// Per-call options.
#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "protobus/abort.h"

namespace protobus {

// Options for a unary call or a fire-and-forget publish.
struct CallOptions {
  // Free-text caller identity, carried in the request envelope for tracing.
  // It is not authenticated by anything.
  std::string actor;
  // false: publish without waiting for a reply. The call returns once the
  // broker has confirmed the request.
  bool rpc = true;
  // How long the call may take. Default Config::rpcCallTimeoutMs(). The
  // deadline starts once the connection is ready to publish (a call made
  // during a reconnection first waits for it, up to
  // Config::connectionReadyTimeoutMs()) and bounds the broker confirm as well
  // as the reply.
  std::optional<int64_t> timeoutMs;
  // AMQP message priority, 0-255. Only takes effect on a queue declared with
  // maxPriority; the broker ignores it elsewhere.
  std::optional<int> priority;
  // The message's identity, as the consumer sees it in its context. Default:
  // a fresh UUID. Set it to make a caller-driven republish recognisable after
  // an AMBIGUOUS failure (PublishConfirmTimeoutError, ChannelClosedError):
  // the same id on the second attempt is what lets an idempotent consumer see
  // one message. Derive it from the work (an order id), never from a clock.
  // Refused when blank, and when longer than 255 bytes.
  std::optional<std::string> messageId;
};

// Options for a streaming call.
struct StreamOptions {
  std::string actor;
  // The longest gap tolerated between chunks. Default
  // Config::streamIdleTimeoutMs().
  std::optional<int64_t> idleTimeoutMs;
  // Cancels the stream when it fires, from anywhere. Ending the iteration
  // early (or destroying the stream) cancels too.
  std::optional<AbortSignal> signal;
};

}  // namespace protobus
