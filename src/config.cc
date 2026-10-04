#include "protobus/config.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <optional>
#include <unordered_map>

namespace protobus {
namespace detail {

namespace {

std::string trim(const std::string& s) {
  auto begin = std::find_if_not(s.begin(), s.end(), [](unsigned char c) { return std::isspace(c); });
  auto end = std::find_if_not(s.rbegin(), s.rend(), [](unsigned char c) { return std::isspace(c); }).base();
  return begin < end ? std::string(begin, end) : std::string();
}

std::optional<std::string> rawEnv(const char* name) {
  const char* v = std::getenv(name);
  if (v == nullptr) return std::nullopt;
  return std::string(v);
}

struct CacheEntry {
  std::optional<std::string> raw;
  int64_t value;
};

// These getters sit on per-message paths, so a parse is memoised against the
// raw string and redone only when the variable actually changes.
std::mutex cacheMutex;
std::unordered_map<std::string, CacheEntry>& cache() {
  static std::unordered_map<std::string, CacheEntry> c;
  return c;
}

}  // namespace

int64_t envInt(const char* name, int64_t fallback) {
  auto raw = rawEnv(name);
  {
    std::lock_guard<std::mutex> lock(cacheMutex);
    auto it = cache().find(name);
    if (it != cache().end() && it->second.raw == raw) {
      return it->second.value;
    }
  }
  int64_t value = fallback;
  if (raw) {
    const std::string text = trim(*raw);
    const bool digits = !text.empty() && std::all_of(text.begin(), text.end(), [](unsigned char c) {
      return c >= '0' && c <= '9';
    });
    // At most 15 digits: comfortably inside int64 and inside the range a
    // millisecond duration can be added to a clock without overflowing.
    if (digits && text.size() <= 15) {
      const int64_t parsed = std::stoll(text);
      if (parsed > 0) value = parsed;
    }
  }
  std::lock_guard<std::mutex> lock(cacheMutex);
  cache()[name] = CacheEntry{raw, value};
  return value;
}

bool envBool(const char* name, bool fallback) {
  auto raw = rawEnv(name);
  if (!raw) return fallback;
  std::string v = trim(*raw);
  std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) { return std::tolower(c); });
  if (v == "1" || v == "true" || v == "yes" || v == "on") return true;
  if (v == "0" || v == "false" || v == "no" || v == "off") return false;
  return fallback;
}

std::string envString(const char* name, const std::string& fallback) {
  auto raw = rawEnv(name);
  if (!raw || raw->empty()) return fallback;
  return *raw;
}

}  // namespace detail

bool Config::exposeInternalErrors() { return detail::envBool("PROTOBUS_EXPOSE_INTERNAL_ERRORS", true); }

std::string Config::busExchangeName() { return detail::envString("BUS_EXCHANGE_NAME", "proto.bus"); }

std::string Config::callbacksExchangeName() {
  return detail::envString("CALLBACKS_EXCHANGE_NAME", "proto.bus.callback");
}

std::string Config::cancelExchangeName() { return detail::envString("CANCEL_EXCHANGE_NAME", "proto.bus.cancel"); }

std::string Config::eventsExchangeName() { return detail::envString("EVENTS_EXCHANGE_NAME", "proto.bus.events"); }

int64_t Config::messageProcessingTimeout() { return detail::envInt("MESSAGE_PROCESSING_TIMEOUT", 600000); }

int64_t Config::rpcCallTimeoutMs() { return detail::envInt("RPC_CALL_TIMEOUT_MS", 600000); }

int64_t Config::streamIdleTimeoutMs() { return detail::envInt("STREAM_IDLE_TIMEOUT_MS", 60000); }

int64_t Config::defaultPrefetch() { return std::min<int64_t>(detail::envInt("DEFAULT_PREFETCH", 1), 65535); }

int64_t Config::publishConfirmTimeoutMs() { return detail::envInt("PUBLISH_CONFIRM_TIMEOUT_MS", 30000); }

int64_t Config::heartbeatSeconds() { return std::min<int64_t>(detail::envInt("AMQP_HEARTBEAT_SECONDS", 30), 65535); }

int64_t Config::connectionReadyTimeoutMs() { return detail::envInt("CONNECTION_READY_TIMEOUT_MS", 30000); }

int64_t Config::maxOutstandingConfirms() {
  return std::min<int64_t>(detail::envInt("MAX_OUTSTANDING_CONFIRMS", 256), 65535);
}

int64_t Config::streamMaxBufferedChunks() { return detail::envInt("STREAM_MAX_BUFFERED_CHUNKS", 1024); }

int64_t Config::streamMaxBufferedBytes() { return detail::envInt("STREAM_MAX_BUFFERED_BYTES", 64LL * 1024 * 1024); }

int64_t Config::streamMaxTotalBufferedBytes() {
  return detail::envInt("STREAM_MAX_TOTAL_BUFFERED_BYTES", 256LL * 1024 * 1024);
}

int64_t Config::shutdownDrainTimeoutMs() { return detail::envInt("SHUTDOWN_DRAIN_TIMEOUT_MS", 30000); }

int64_t Config::shutdownExitGraceMs() { return detail::envInt("SHUTDOWN_EXIT_GRACE_MS", 5000); }

}  // namespace protobus
