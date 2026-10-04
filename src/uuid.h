// RFC 4122 version 4 UUIDs, the default message and correlation ids.
#pragma once

#include <string>

namespace protobus::detail {

// A random version-4 UUID in canonical lowercase form.
std::string randomUuid();

}  // namespace protobus::detail
