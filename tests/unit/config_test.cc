#include <gtest/gtest.h>

#include <cstdlib>

#include "protobus/config.h"

namespace protobus {
namespace {

struct EnvGuard {
  explicit EnvGuard(const char* n) : name(n) {
    if (const char* v = std::getenv(n)) old = v;
  }
  ~EnvGuard() {
    if (old) {
      ::setenv(name, old->c_str(), 1);
    } else {
      ::unsetenv(name);
    }
  }
  const char* name;
  std::optional<std::string> old;
};

TEST(Config, DefaultsMatchTheOtherPorts) {
  EXPECT_EQ(Config::busExchangeName(), "proto.bus");
  EXPECT_EQ(Config::callbacksExchangeName(), "proto.bus.callback");
  EXPECT_EQ(Config::eventsExchangeName(), "proto.bus.events");
  EXPECT_EQ(Config::cancelExchangeName(), "proto.bus.cancel");
  EXPECT_EQ(Config::messageProcessingTimeout(), 600000);
  EXPECT_EQ(Config::rpcCallTimeoutMs(), 600000);
  EXPECT_EQ(Config::streamIdleTimeoutMs(), 60000);
  EXPECT_EQ(Config::defaultPrefetch(), 1);
  EXPECT_EQ(Config::publishConfirmTimeoutMs(), 30000);
  EXPECT_EQ(Config::heartbeatSeconds(), 30);
  EXPECT_EQ(Config::connectionReadyTimeoutMs(), 30000);
  EXPECT_EQ(Config::maxOutstandingConfirms(), 256);
  EXPECT_EQ(Config::streamMaxBufferedChunks(), 1024);
  EXPECT_EQ(Config::streamMaxBufferedBytes(), 64 * 1024 * 1024);
  EXPECT_EQ(Config::streamMaxTotalBufferedBytes(), 256 * 1024 * 1024);
  EXPECT_TRUE(Config::exposeInternalErrors());
  EXPECT_EQ(Config::PRIORITY_NORMAL, 0);
  EXPECT_EQ(Config::PRIORITY_HIGH, 1);
  EXPECT_EQ(Config::PRIORITY_CONTROL, 2);
  EXPECT_EQ(Config::RECOMMENDED_MAX_PRIORITY, 2);
}

TEST(Config, IntegersMustBeAllDigitsAndPositive) {
  EnvGuard g("RPC_CALL_TIMEOUT_MS");
  // A typo like 6oo000 must keep the default, not become 6 or NaN.
  for (const char* bad : {"6oo000", "123abc", "-5", "0", "", "  ", "1.5", "99999999999999999999"}) {
    ::setenv("RPC_CALL_TIMEOUT_MS", bad, 1);
    EXPECT_EQ(Config::rpcCallTimeoutMs(), 600000) << bad;
  }
  ::setenv("RPC_CALL_TIMEOUT_MS", " 1500 ", 1);
  EXPECT_EQ(Config::rpcCallTimeoutMs(), 1500);
  // Changed at runtime: picked up.
  ::setenv("RPC_CALL_TIMEOUT_MS", "2500", 1);
  EXPECT_EQ(Config::rpcCallTimeoutMs(), 2500);
}

TEST(Config, BooleansNeedAnExplicitValue) {
  EnvGuard g("PROTOBUS_EXPOSE_INTERNAL_ERRORS");
  for (const char* off : {"false", "0", "no", "OFF", " False "}) {
    ::setenv("PROTOBUS_EXPOSE_INTERNAL_ERRORS", off, 1);
    EXPECT_FALSE(Config::exposeInternalErrors()) << off;
  }
  ::setenv("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "maybe", 1);
  EXPECT_TRUE(Config::exposeInternalErrors());
}

TEST(Config, ExchangeNamesFromTheEnvironment) {
  EnvGuard g("BUS_EXCHANGE_NAME");
  ::setenv("BUS_EXCHANGE_NAME", "custom.bus", 1);
  EXPECT_EQ(Config::busExchangeName(), "custom.bus");
  ::setenv("BUS_EXCHANGE_NAME", "", 1);
  EXPECT_EQ(Config::busExchangeName(), "proto.bus");
}

}  // namespace
}  // namespace protobus
