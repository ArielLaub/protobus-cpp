// Logging, mirroring the TypeScript logger.
//
// `Logger` is the plain sink every framework line goes through: four
// severities, a process-wide level filter (LOG_LEVEL, default info; debug is
// off by default because payload-level detail is opt-in), and a replaceable
// sink installed with setLogger().
//
// `Log` is the structured counterpart. A call site describes what happened as
// a LogRecord; a sink implementing IStructuredLogger receives the record, and
// a plain ILogger receives it rendered by formatLogRecord() instead. Records
// carry only framework-generated text and low-cardinality identifiers: URLs,
// headers, payloads and broker error strings are absent. The only route for
// payload material is `diagnostics`, which is assembled only when the
// application installs a serializer with setDiagnosticsSerializer().
#pragma once

#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>

namespace google::protobuf {
class Message;
}

namespace protobus {

class ILogger {
 public:
  virtual ~ILogger() = default;
  virtual void info(const std::string& message) = 0;
  virtual void warn(const std::string& message) = 0;
  virtual void debug(const std::string& message) = 0;
  virtual void error(const std::string& message) = 0;
};

// Severity threshold. Lower values are more verbose.
enum class LogLevel : int {
  Debug = 10,
  Info = 20,
  Warn = 30,
  Error = 40,
  Silent = 100,
};

// Writes info/debug to stdout and warn/error to stderr, one line each.
class DefaultLogger : public ILogger {
 public:
  void info(const std::string& message) override;
  void warn(const std::string& message) override;
  void debug(const std::string& message) override;
  void error(const std::string& message) override;
};

// Install a sink. Level filtering happens before the sink is called, so a
// custom sink never sees suppressed lines. nullptr restores the default.
void setLogger(std::shared_ptr<ILogger> logger);
void setLogLevel(LogLevel level);
LogLevel getLogLevel();

// The level-filtered front end every framework line goes through.
namespace Logger {
void debug(const std::string& message);
void info(const std::string& message);
void warn(const std::string& message);
void error(const std::string& message);
bool enabled(LogLevel level);
}  // namespace Logger

// ---- structured logging --------------------------------------------------------

enum class LogOutcome {
  Ok,
  Confirmed,
  Failed,
  Timeout,
  Retried,
  Rejected,
  Dropped,
  Unroutable,
};

const char* toString(LogOutcome outcome);

// Raw material offered to the diagnostics serializer, never logged as-is.
struct LogDiagnostics {
  const google::protobuf::Message* payload = nullptr;
  const std::string* body = nullptr;
  const std::exception* error = nullptr;
  std::map<std::string, std::string> extra;
};

// Fields a call site attaches to a structured line.
struct LogFields {
  std::string operation;
  std::optional<std::string> messageType;
  std::optional<std::string> messageId;
  std::optional<std::string> correlationId;
  std::optional<std::string> service;
  std::optional<std::string> method;
  std::optional<std::string> queue;
  std::optional<std::string> exchange;
  std::optional<std::string> routingKey;
  std::optional<std::string> errorCode;
  std::optional<std::string> errorName;
  std::optional<LogOutcome> outcome;
  std::optional<double> sizeBytes;
  std::optional<double> durationMs;
  std::optional<double> attempt;
  // Lazily produces payload-level material. Invoked only when a serializer is
  // installed and the line passes the level filter.
  std::function<LogDiagnostics()> diagnostics;
};

// One framework log line as data.
struct LogRecord {
  std::string component = "protobus";
  std::string level;      // debug | info | warn | error
  std::string timestamp;  // ISO 8601, UTC, generated at emit time
  std::string operation;
  std::string message;
  std::optional<std::string> messageType;
  std::optional<std::string> messageId;
  std::optional<std::string> correlationId;
  std::optional<std::string> service;
  std::optional<std::string> method;
  std::optional<std::string> queue;
  std::optional<std::string> exchange;
  std::optional<std::string> routingKey;
  std::optional<std::string> errorCode;
  std::optional<std::string> errorName;
  std::optional<std::string> outcome;
  std::optional<double> sizeBytes;
  std::optional<double> durationMs;
  std::optional<double> attempt;
  // Whatever the installed diagnostics serializer returned, if anything.
  std::optional<std::string> diagnostics;
};

// A sink that accepts records rather than only formatted strings.
class IStructuredLogger : public ILogger {
 public:
  virtual void log(const LogRecord& record) = 0;
};

// Decides what, if anything, of a line's payload material is safe to log.
// Returning nullopt omits the field. The application owns the redaction
// policy here.
using DiagnosticsSerializer = std::function<std::optional<std::string>(const LogDiagnostics&, const LogRecord&)>;

void setDiagnosticsSerializer(DiagnosticsSerializer serializer);

// Render a record as the single line a string-only sink receives.
std::string formatLogRecord(const LogRecord& record);

namespace Log {
void debug(const std::string& message, const LogFields& fields);
void info(const std::string& message, const LogFields& fields);
void warn(const std::string& message, const LogFields& fields);
void error(const std::string& message, const LogFields& fields);
}  // namespace Log

// Strip the password out of a broker URL so it is safe to log. Anything that
// does not look like a URL is reported as `<redacted>`.
std::string redactUrl(const std::string& url);

}  // namespace protobus
