#include "protobus/priority.h"

#include <string>

#include "protobus/config.h"
#include "protobus/errors.h"

namespace protobus {

namespace {

std::optional<int> requireInRange(std::optional<int> value, const char* label, int min, int max,
                                  const std::string& extra) {
  if (!value) return std::nullopt;
  if (*value < min || *value > max) {
    throw InvalidPriorityError(std::string(label) + " must be between " + std::to_string(min) + " and " +
                               std::to_string(max) + ", got " + std::to_string(*value) + ". " + extra);
  }
  return value;
}

}  // namespace

std::optional<int> validateMaxPriority(std::optional<int> value) {
  const int rec = Config::RECOMMENDED_MAX_PRIORITY;
  return requireInRange(value, "maxPriority", 1, 255,
                        "RabbitMQ maintains internal structures per priority level, so keep the range small - " +
                            std::to_string(rec) + " is the recommended value and gives " + std::to_string(rec + 1) +
                            " levels.");
}

std::optional<int> validatePriority(std::optional<int> value) {
  return requireInRange(value, "priority", 0, 255,
                        "A priority above the queue's x-max-priority is clamped by the broker, not rejected.");
}

}  // namespace protobus
