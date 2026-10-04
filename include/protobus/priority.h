// Message priority (opt-in).
//
// `maxPriority` on MessageServiceOptions declares a service's request queue as
// a RabbitMQ priority queue; `priority` on CallOptions sets a single message's
// level. Both are absent from the wire unless set, so an upgraded process
// talks to an un-upgraded one unchanged.
#pragma once

#include <optional>

namespace protobus {

// Validate the queue-level maxPriority, the value that becomes x-max-priority.
// The floor is 1: x-max-priority 0 gives a single level, a plain queue with a
// priority queue's overhead, which nobody wants on purpose. Throws
// InvalidPriorityError outside 1..255.
std::optional<int> validateMaxPriority(std::optional<int> value);

// Validate a per-message priority. 0 is valid and is RabbitMQ's default.
// Throws InvalidPriorityError outside 0..255.
std::optional<int> validatePriority(std::optional<int> value);

}  // namespace protobus
