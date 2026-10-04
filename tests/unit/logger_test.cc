#include <gtest/gtest.h>

#include <mutex>
#include <vector>

#include "amqp/url.h"
#include "protobus/logger.h"
#include "protobus/transport.h"

namespace protobus {
namespace {

class Capture : public ILogger {
 public:
  void info(const std::string& m) override { add("info", m); }
  void warn(const std::string& m) override { add("warn", m); }
  void debug(const std::string& m) override { add("debug", m); }
  void error(const std::string& m) override { add("error", m); }
  std::vector<std::pair<std::string, std::string>> lines;

 private:
  void add(const char* l, const std::string& m) {
    std::lock_guard<std::mutex> lock(mutex_);
    lines.emplace_back(l, m);
  }
  std::mutex mutex_;
};

class StructuredCapture : public IStructuredLogger {
 public:
  void info(const std::string&) override {}
  void warn(const std::string&) override {}
  void debug(const std::string&) override {}
  void error(const std::string&) override {}
  void log(const LogRecord& r) override { records.push_back(r); }
  std::vector<LogRecord> records;
};

struct LoggerTest : ::testing::Test {
  void TearDown() override {
    setLogger(nullptr);
    setLogLevel(LogLevel::Info);
    setDiagnosticsSerializer(nullptr);
  }
};

TEST_F(LoggerTest, LevelFilterRunsBeforeTheSink) {
  auto sink = std::make_shared<Capture>();
  setLogger(sink);
  setLogLevel(LogLevel::Warn);
  Logger::debug("d");
  Logger::info("i");
  Logger::warn("w");
  Logger::error("e");
  ASSERT_EQ(sink->lines.size(), 2u);
  EXPECT_EQ(sink->lines[0].first, "warn");
  setLogLevel(LogLevel::Silent);
  Logger::error("x");
  EXPECT_EQ(sink->lines.size(), 2u);
}

TEST_F(LoggerTest, StructuredLinesRenderForAStringSink) {
  auto sink = std::make_shared<Capture>();
  setLogger(sink);
  LogFields f;
  f.operation = "publish";
  f.messageType = "pkg.Svc.add";
  f.correlationId = "c-1";
  f.outcome = LogOutcome::Confirmed;
  f.sizeBytes = 12;
  Log::info("published request", f);
  ASSERT_EQ(sink->lines.size(), 1u);
  EXPECT_EQ(sink->lines[0].second,
            "[protobus] publish: published request (messageType=pkg.Svc.add correlationId=c-1 outcome=confirmed "
            "sizeBytes=12)");
}

TEST_F(LoggerTest, ControlCharactersCannotForgeALine) {
  auto sink = std::make_shared<Capture>();
  setLogger(sink);
  LogFields f;
  f.operation = "consume";
  f.queue = "q\n[protobus] fake: forged";
  Log::warn("x", f);
  EXPECT_EQ(sink->lines[0].second.find('\n'), std::string::npos);
}

TEST_F(LoggerTest, DiagnosticsAreAssembledOnlyWithASerializer) {
  auto sink = std::make_shared<StructuredCapture>();
  setLogger(sink);
  int assembled = 0;
  LogFields f;
  f.operation = "consume";
  f.diagnostics = [&] {
    ++assembled;
    return LogDiagnostics{};
  };
  Log::info("m", f);
  EXPECT_EQ(assembled, 0);
  ASSERT_EQ(sink->records.size(), 1u);
  EXPECT_FALSE(sink->records[0].diagnostics);
  setDiagnosticsSerializer([](const LogDiagnostics&, const LogRecord&) { return std::string("redacted"); });
  Log::info("m", f);
  EXPECT_EQ(assembled, 1);
  EXPECT_EQ(sink->records[1].diagnostics, "redacted");
  EXPECT_EQ(sink->records[1].component, "protobus");
}

TEST(RedactUrl, HidesThePasswordOnly) {
  EXPECT_EQ(redactUrl("amqp://user:s3cret@host:5672/%2f"), "amqp://user:***@host:5672/%2f");
  EXPECT_EQ(redactUrl("amqp://host/"), "amqp://host/");
  EXPECT_EQ(redactUrl("not a url"), "<redacted>");
}

TEST(BrokerUrl, ParsesTheRabbitMQUriSpec) {
  auto u = amqp::parseBrokerUrl("amqps://us%40er:p%3Ass@broker.example:5999/%2fprod?heartbeat=10&verify=verify_none");
  EXPECT_TRUE(u.tls);
  EXPECT_EQ(u.user, "us@er");
  EXPECT_EQ(u.password, "p:ss");
  EXPECT_EQ(u.host, "broker.example");
  EXPECT_EQ(u.port, 5999);
  EXPECT_EQ(u.vhost, "/prod");
  EXPECT_EQ(u.query.at("heartbeat"), "10");
  EXPECT_EQ(u.query.at("verify"), "verify_none");

  auto d = amqp::parseBrokerUrl("amqp://localhost");
  EXPECT_EQ(d.port, 5672);
  EXPECT_EQ(d.vhost, "/");
  EXPECT_EQ(d.user, "guest");

  auto v6 = amqp::parseBrokerUrl("amqp://[::1]:5673/");
  EXPECT_EQ(v6.host, "::1");
  EXPECT_EQ(v6.port, 5673);

  EXPECT_THROW(amqp::parseBrokerUrl("http://x"), amqp::AmqpError);
  EXPECT_THROW(amqp::parseBrokerUrl("amqp://h:99999/"), amqp::AmqpError);
}

TEST(FieldValue, ReadsEveryEncodingPeersProduce) {
  using amqp::FieldValue;
  EXPECT_EQ(FieldValue::fromInt(3).asInt(), 3);
  EXPECT_EQ(FieldValue::fromInt32(3).asInt(), 3);
  EXPECT_EQ(FieldValue::fromString("7").asInt(), 7);
  EXPECT_FALSE(FieldValue::fromString("x7").asInt());
  EXPECT_EQ(FieldValue::fromDouble(4.0).asInt(), 4);
  EXPECT_EQ(FieldValue::fromBool(true).asBool(), true);
  EXPECT_EQ(FieldValue::fromInt(0).asBool(), false);
  EXPECT_EQ(FieldValue::fromString("TRUE").asBool(), true);
  EXPECT_EQ(FieldValue::fromString("1").asBool(), true);
  EXPECT_EQ(FieldValue::fromString("no").asBool(), false);
}

}  // namespace
}  // namespace protobus
