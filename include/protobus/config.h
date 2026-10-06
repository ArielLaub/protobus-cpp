// Environment-backed configuration, mirroring the TypeScript and Python
// `Config`.
//
// Every getter reads the environment on each call (memoised per raw value), so
// a variable changed at runtime, or by a test, is picked up. Integer parsing is
// strict: the value must be all digits and positive, or the default is kept.
// A boolean must be one of 1/true/yes/on or 0/false/no/off, case-insensitive.
// Anything malformed keeps the default rather than becoming a surprising zero.
//
// The exchange names are part of the wire protocol: every process on one bus,
// whatever its language, must agree on them.
#pragma once

#include <cstdint>
#include <string>

namespace protobus {

class Config {
 public:
  // Send the message of an UNHANDLED service error back to the caller. On by
  // default. Turn it off (PROTOBUS_EXPOSE_INTERNAL_ERRORS=false) for a service
  // whose callers relay errors to untrusted clients; callers then see a
  // generic message naming the correlation id, and the real error stays in
  // the service's own log. A HandledError always crosses.
  static bool exposeInternalErrors();

  // RPC requests (topic). BUS_EXCHANGE_NAME, default proto.bus
  static std::string busExchangeName();
  // RPC replies (direct). CALLBACKS_EXCHANGE_NAME, default proto.bus.callback
  static std::string callbacksExchangeName();
  // Stream-cancellation notices (fanout). CANCEL_EXCHANGE_NAME, default
  // proto.bus.cancel. Fanout, because a cancel has to reach the one replica
  // running that stream and the caller cannot know which one it is.
  static std::string cancelExchangeName();
  // Events (topic). EVENTS_EXCHANGE_NAME, default proto.bus.events
  static std::string eventsExchangeName();

  // How long a service spends on one unary request before the attempt is
  // failed and retried. MESSAGE_PROCESSING_TIMEOUT, default 600000 ms.
  static int64_t messageProcessingTimeout();
  // How long a unary caller waits for its reply. RPC_CALL_TIMEOUT_MS,
  // default 600000 ms.
  static int64_t rpcCallTimeoutMs();
  // The longest gap a streaming caller tolerates between chunks.
  // STREAM_IDLE_TIMEOUT_MS, default 60000 ms.
  static int64_t streamIdleTimeoutMs();
  // Prefetch for late-ack consumers that set no concurrency of their own.
  // DEFAULT_PREFETCH, default 1.
  static int64_t defaultPrefetch();
  // How long a publish waits for its broker confirm. Expiry is AMBIGUOUS.
  // PUBLISH_CONFIRM_TIMEOUT_MS, default 30000 ms.
  static int64_t publishConfirmTimeoutMs();
  // AMQP heartbeat interval. A `heartbeat` in the broker URL wins, and
  // `heartbeat=0` there disables heartbeats. AMQP_HEARTBEAT_SECONDS, default 30.
  static int64_t heartbeatSeconds();
  // How long a publish parked on a reconnection waits before failing with
  // NotReadyError. CONNECTION_READY_TIMEOUT_MS, default 30000 ms.
  static int64_t connectionReadyTimeoutMs();
  // Publishes the broker has not yet answered, on one channel at a time. A
  // publish whose confirm timed out keeps its place until the broker does
  // answer (or the channel closes): it may still be stored.
  // MAX_OUTSTANDING_CONFIRMS, default 256.
  static int64_t maxOutstandingConfirms();
  // Publishes waiting, on one channel, for a place under that bound. One
  // more fails at once with PublishBacklogError. MAX_PARKED_PUBLISHES,
  // default 4096.
  static int64_t maxParkedPublishes();
  // Streaming caller buffer bounds. Crossing one fails the stream with
  // StreamBackpressureError rather than growing without limit.
  static int64_t streamMaxBufferedChunks();      // STREAM_MAX_BUFFERED_CHUNKS, 1024
  static int64_t streamMaxBufferedBytes();       // STREAM_MAX_BUFFERED_BYTES, 64 MiB
  static int64_t streamMaxTotalBufferedBytes();  // STREAM_MAX_TOTAL_BUFFERED_BYTES, 256 MiB
  // How long a graceful shutdown waits for in-flight work.
  // SHUTDOWN_DRAIN_TIMEOUT_MS, default 30000 ms.
  static int64_t shutdownDrainTimeoutMs();
  // How long RunnableService waits, after shutting down, for the process to
  // leave on its own before forcing an exit. SHUTDOWN_EXIT_GRACE_MS, 5000 ms.
  static int64_t shutdownExitGraceMs();

  // Named message-priority levels, matching the other ports. PRIORITY_NORMAL
  // is 0 because that is how RabbitMQ sorts a message carrying no priority at
  // all, so an old publisher and a new one passing PRIORITY_NORMAL sort
  // identically on the same queue.
  static constexpr int PRIORITY_NORMAL = 0;
  static constexpr int PRIORITY_HIGH = 1;
  static constexpr int PRIORITY_CONTROL = 2;
  // The maxPriority that gives the three levels above. RabbitMQ keeps
  // internal structures per level, so keep it small.
  static constexpr int RECOMMENDED_MAX_PRIORITY = 2;

  // Headers of the server-streaming wire protocol.
  static constexpr const char* HEADER_FINAL = "x-protobus-final";
  static constexpr const char* HEADER_SEQ = "x-protobus-seq";
};

namespace detail {
// The TS envInt grammar: trimmed, ^\d+$, > 0, in range; otherwise `fallback`.
int64_t envInt(const char* name, int64_t fallback);
// 1/true/yes/on or 0/false/no/off; otherwise `fallback`.
bool envBool(const char* name, bool fallback);
// The variable when set and non-empty; otherwise `fallback`.
std::string envString(const char* name, const std::string& fallback);
}  // namespace detail

}  // namespace protobus
