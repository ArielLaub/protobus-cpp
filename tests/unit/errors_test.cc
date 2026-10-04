#include <gtest/gtest.h>

#include <cstdlib>

#include "protobus/config.h"
#include "protobus/errors.h"

namespace protobus {
namespace {

class ValidationError : public HandledError {
 public:
  explicit ValidationError(const std::string& m) : HandledError(m, "VALIDATION_ERROR") {}
};

TEST(Errors, HandledErrorsAreRecognisedThroughSubclasses) {
  EXPECT_TRUE(isHandledError(HandledError("x")));
  EXPECT_TRUE(isHandledError(ValidationError("x")));
  EXPECT_TRUE(isHandledError(ProtocolError("x")));
  EXPECT_TRUE(isHandledError(InvalidMethodError("x")));
  EXPECT_FALSE(isHandledError(std::runtime_error("x")));
  EXPECT_FALSE(isHandledError(RemoteError("m", "x", "CODE")));
  EXPECT_EQ(HandledError("x").code(), "HANDLED_ERROR");
  EXPECT_EQ(HandledError("x", "").code(), "HANDLED_ERROR");
  EXPECT_EQ(ProtocolError("x").code(), "PROTOCOL_ERROR");
  EXPECT_EQ(InvalidMethodError("x").name(), "InvalidMethodError");
}

TEST(Errors, PublishOutcomesSayWhetherTheyAreAmbiguous) {
  EXPECT_FALSE(PublishNackedError("m", "id").ambiguous());
  EXPECT_FALSE(UnroutableError("m", "id").ambiguous());
  EXPECT_TRUE(PublishConfirmTimeoutError("m", "id").ambiguous());
  EXPECT_TRUE(ChannelClosedError("m", "id").ambiguous());
  EXPECT_EQ(UnroutableError("m", "id").messageId(), "id");
  EXPECT_EQ(UnroutableError("m", "id").code(), "UNROUTABLE");
}

TEST(Errors, SafeSummaryNeverQuotesAnUnhandledMessage) {
  const std::runtime_error leaky("password=hunter2");
  EXPECT_EQ(safeErrorSummary(&leaky), "std::runtime_error");
  const TimeoutError timeout("message abc exceeded");
  EXPECT_EQ(safeErrorSummary(&timeout), "TimeoutError[PROCESSING_TIMEOUT]");
  const ValidationError handled("name is required");
  // A HandledError's message is kept: exposing it was the point.
  EXPECT_EQ(safeErrorSummary(&handled), "HandledError[VALIDATION_ERROR]: name is required");
  EXPECT_EQ(safeErrorSummary(nullptr), "UnknownError");
}

TEST(Errors, SanitisingForTheCaller) {
  const std::runtime_error internal("db at 10.0.0.1 refused");
  ::setenv("PROTOBUS_EXPOSE_INTERNAL_ERRORS", "false", 1);
  auto hidden = sanitizeErrorForClient(internal, "corr-1");
  EXPECT_EQ(hidden.code, "INTERNAL_ERROR");
  EXPECT_EQ(hidden.message, "internal service error (correlationId corr-1)");
  // A HandledError and a processing timeout cross either way.
  EXPECT_EQ(sanitizeErrorForClient(ValidationError("bad")).message, "bad");
  EXPECT_EQ(sanitizeErrorForClient(TimeoutError("late")).code, "PROCESSING_TIMEOUT");
  ::unsetenv("PROTOBUS_EXPOSE_INTERNAL_ERRORS");
  auto shown = sanitizeErrorForClient(internal);
  EXPECT_EQ(shown.message, "db at 10.0.0.1 refused");
  EXPECT_EQ(shown.code, "");
}

}  // namespace
}  // namespace protobus
