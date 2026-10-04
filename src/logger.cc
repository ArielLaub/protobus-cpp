#include "protobus/logger.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <mutex>
#include <sstream>

namespace protobus {

namespace {

LogLevel levelFromEnv() {
  const char* raw = std::getenv("LOG_LEVEL");
  std::string v = raw ? raw : "";
  v.erase(std::remove_if(v.begin(), v.end(), [](unsigned char c) { return std::isspace(c); }), v.end());
  std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) { return std::tolower(c); });
  if (v == "debug") return LogLevel::Debug;
  if (v == "info") return LogLevel::Info;
  if (v == "warn" || v == "warning") return LogLevel::Warn;
  if (v == "error") return LogLevel::Error;
  if (v == "silent" || v == "off" || v == "none") return LogLevel::Silent;
  return LogLevel::Info;
}

std::atomic<int> currentLevel{static_cast<int>(levelFromEnv())};

// Process-wide state is never destroyed: worker threads may still log while
// static destructors run at exit.
std::mutex& sinkMutex() {
  static auto* m = new std::mutex;
  return *m;
}
std::shared_ptr<ILogger>& sinkSlot() {
  static auto* sink = new std::shared_ptr<ILogger>(std::make_shared<DefaultLogger>());
  return *sink;
}

std::shared_ptr<ILogger> currentSink() {
  std::lock_guard<std::mutex> lock(sinkMutex());
  return sinkSlot();
}

std::mutex& serializerMutex() {
  static auto* m = new std::mutex;
  return *m;
}
DiagnosticsSerializer& serializerSlot() {
  static auto* s = new DiagnosticsSerializer;
  return *s;
}

std::mutex& outputMutex() {
  static auto* m = new std::mutex;
  return *m;
}

void writeLine(std::ostream& out, const std::string& message) {
  std::lock_guard<std::mutex> lock(outputMutex());
  out << message << '\n';
  out.flush();
}

}  // namespace

void DefaultLogger::info(const std::string& message) { writeLine(std::cout, message); }
void DefaultLogger::warn(const std::string& message) { writeLine(std::cerr, message); }
void DefaultLogger::debug(const std::string& message) { writeLine(std::cout, message); }
void DefaultLogger::error(const std::string& message) { writeLine(std::cerr, message); }

void setLogger(std::shared_ptr<ILogger> logger) {
  std::lock_guard<std::mutex> lock(sinkMutex());
  sinkSlot() = logger ? std::move(logger) : std::make_shared<DefaultLogger>();
}

void setLogLevel(LogLevel level) { currentLevel.store(static_cast<int>(level)); }

LogLevel getLogLevel() { return static_cast<LogLevel>(currentLevel.load()); }

namespace Logger {

bool enabled(LogLevel level) { return currentLevel.load() <= static_cast<int>(level); }

void debug(const std::string& message) {
  if (enabled(LogLevel::Debug)) currentSink()->debug(message);
}
void info(const std::string& message) {
  if (enabled(LogLevel::Info)) currentSink()->info(message);
}
void warn(const std::string& message) {
  if (enabled(LogLevel::Warn)) currentSink()->warn(message);
}
void error(const std::string& message) {
  if (enabled(LogLevel::Error)) currentSink()->error(message);
}

}  // namespace Logger

// ---- structured logging --------------------------------------------------------

const char* toString(LogOutcome outcome) {
  switch (outcome) {
    case LogOutcome::Ok:
      return "ok";
    case LogOutcome::Confirmed:
      return "confirmed";
    case LogOutcome::Failed:
      return "failed";
    case LogOutcome::Timeout:
      return "timeout";
    case LogOutcome::Retried:
      return "retried";
    case LogOutcome::Rejected:
      return "rejected";
    case LogOutcome::Dropped:
      return "dropped";
    case LogOutcome::Unroutable:
      return "unroutable";
  }
  return "unknown";
}

void setDiagnosticsSerializer(DiagnosticsSerializer serializer) {
  std::lock_guard<std::mutex> lock(serializerMutex());
  serializerSlot() = std::move(serializer);
}

namespace {

constexpr size_t kFieldMaxLength = 256;
constexpr size_t kMessageMaxLength = 1024;

// Control characters collapse to one space so a value carrying a newline
// cannot forge a second log line; long values are truncated so one field
// cannot flood the log.
std::optional<std::string> sanitizeValue(const std::optional<std::string>& value, size_t maxLength) {
  if (!value) return std::nullopt;
  std::string out;
  out.reserve(value->size());
  bool inControl = false;
  for (unsigned char c : *value) {
    if (c < 0x20 || c == 0x7f) {
      if (!inControl) out.push_back(' ');
      inControl = true;
    } else {
      out.push_back(static_cast<char>(c));
      inControl = false;
    }
  }
  auto begin = out.find_first_not_of(' ');
  if (begin == std::string::npos) return std::nullopt;
  auto end = out.find_last_not_of(' ');
  out = out.substr(begin, end - begin + 1);
  if (out.size() > maxLength) out.resize(maxLength);
  return out;
}

std::optional<double> sanitizeNumber(const std::optional<double>& value) {
  if (!value || !std::isfinite(*value)) return std::nullopt;
  return value;
}

std::string isoNow() {
  using namespace std::chrono;
  const auto now = system_clock::now();
  const auto ms = duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000;
  const std::time_t t = system_clock::to_time_t(now);
  std::tm tm{};
  gmtime_r(&t, &tm);
  char buf[32];
  std::strftime(buf, sizeof buf, "%Y-%m-%dT%H:%M:%S", &tm);
  char out[40];
  std::snprintf(out, sizeof out, "%s.%03dZ", buf, static_cast<int>(ms));
  return out;
}

LogRecord buildRecord(const char* level, const std::string& message, const LogFields& fields) {
  LogRecord r;
  r.level = level;
  r.timestamp = isoNow();
  r.operation = sanitizeValue(fields.operation, kFieldMaxLength).value_or("unknown");
  r.message = sanitizeValue(message, kMessageMaxLength).value_or("");
  r.messageType = sanitizeValue(fields.messageType, kFieldMaxLength);
  r.messageId = sanitizeValue(fields.messageId, kFieldMaxLength);
  r.correlationId = sanitizeValue(fields.correlationId, kFieldMaxLength);
  r.service = sanitizeValue(fields.service, kFieldMaxLength);
  r.method = sanitizeValue(fields.method, kFieldMaxLength);
  r.queue = sanitizeValue(fields.queue, kFieldMaxLength);
  r.exchange = sanitizeValue(fields.exchange, kFieldMaxLength);
  r.routingKey = sanitizeValue(fields.routingKey, kFieldMaxLength);
  r.errorCode = sanitizeValue(fields.errorCode, kFieldMaxLength);
  r.errorName = sanitizeValue(fields.errorName, kFieldMaxLength);
  if (fields.outcome) r.outcome = toString(*fields.outcome);
  r.sizeBytes = sanitizeNumber(fields.sizeBytes);
  r.durationMs = sanitizeNumber(fields.durationMs);
  r.attempt = sanitizeNumber(fields.attempt);
  return r;
}

std::string formatNumber(double v) {
  std::ostringstream out;
  if (v == std::floor(v) && std::fabs(v) < 1e15) {
    out << static_cast<long long>(v);
  } else {
    out << v;
  }
  return out.str();
}

void emit(const char* level, LogLevel threshold, const std::string& message, const LogFields& fields) {
  if (!Logger::enabled(threshold)) return;

  LogRecord record = buildRecord(level, message, fields);

  DiagnosticsSerializer serializer;
  {
    std::lock_guard<std::mutex> lock(serializerMutex());
    serializer = serializerSlot();
  }
  if (serializer && fields.diagnostics) {
    // A failing hook, or a payload that cannot be assembled, must not take
    // down the operation being logged or swallow the line itself.
    try {
      auto extra = serializer(fields.diagnostics(), record);
      if (extra) record.diagnostics = std::move(extra);
    } catch (...) {
    }
  }

  auto sink = currentSink();
  if (auto* structured = dynamic_cast<IStructuredLogger*>(sink.get())) {
    try {
      structured->log(record);
      return;
    } catch (...) {
      // A structured sink that throws degrades to the string path rather
      // than losing the line.
    }
  }
  const std::string text = formatLogRecord(record);
  const std::string lv = level;
  if (lv == "debug") {
    sink->debug(text);
  } else if (lv == "info") {
    sink->info(text);
  } else if (lv == "warn") {
    sink->warn(text);
  } else {
    sink->error(text);
  }
}

}  // namespace

std::string formatLogRecord(const LogRecord& r) {
  std::string detail;
  auto add = [&](const char* key, const std::optional<std::string>& v) {
    if (!v) return;
    if (!detail.empty()) detail += ' ';
    detail += key;
    detail += '=';
    detail += *v;
  };
  auto addNum = [&](const char* key, const std::optional<double>& v) {
    if (!v) return;
    if (!detail.empty()) detail += ' ';
    detail += key;
    detail += '=';
    detail += formatNumber(*v);
  };
  add("messageType", r.messageType);
  add("messageId", r.messageId);
  add("correlationId", r.correlationId);
  add("service", r.service);
  add("method", r.method);
  add("queue", r.queue);
  add("exchange", r.exchange);
  add("routingKey", r.routingKey);
  add("errorCode", r.errorCode);
  add("errorName", r.errorName);
  add("outcome", r.outcome);
  addNum("sizeBytes", r.sizeBytes);
  addNum("durationMs", r.durationMs);
  addNum("attempt", r.attempt);
  add("diagnostics", r.diagnostics);
  std::string out = "[" + r.component + "] " + r.operation + ": " + r.message;
  if (!detail.empty()) out += " (" + detail + ")";
  return out;
}

namespace Log {
void debug(const std::string& message, const LogFields& fields) { emit("debug", LogLevel::Debug, message, fields); }
void info(const std::string& message, const LogFields& fields) { emit("info", LogLevel::Info, message, fields); }
void warn(const std::string& message, const LogFields& fields) { emit("warn", LogLevel::Warn, message, fields); }
void error(const std::string& message, const LogFields& fields) { emit("error", LogLevel::Error, message, fields); }
}  // namespace Log

std::string redactUrl(const std::string& url) {
  if (url.empty()) return url;
  const auto scheme = url.find("://");
  if (scheme == std::string::npos || scheme == 0) return "<redacted>";
  for (size_t i = 0; i < scheme; ++i) {
    const unsigned char c = static_cast<unsigned char>(url[i]);
    if (!std::isalnum(c) && c != '+' && c != '-' && c != '.') return "<redacted>";
  }
  const size_t authorityStart = scheme + 3;
  size_t authorityEnd = url.find_first_of("/?#", authorityStart);
  if (authorityEnd == std::string::npos) authorityEnd = url.size();
  const std::string authority = url.substr(authorityStart, authorityEnd - authorityStart);
  const auto at = authority.rfind('@');
  if (at == std::string::npos) return url;
  const std::string userinfo = authority.substr(0, at);
  const auto colon = userinfo.find(':');
  if (colon == std::string::npos || colon + 1 == userinfo.size()) return url;
  return url.substr(0, authorityStart) + userinfo.substr(0, colon) + ":***" + authority.substr(at) +
         url.substr(authorityEnd);
}

}  // namespace protobus
