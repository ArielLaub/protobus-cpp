// Bridges the protobuf C++ API across the versions protobus supports: the
// 3.21 that Debian and Ubuntu ship, and 22 onwards.
//
// The error-collector callbacks changed in protobuf 22: AddError taking
// std::string became RecordError taking absl::string_view. Override with
// PROTOBUS_PB_ERROR(...) and PROTOBUS_PB_TEXT to compile against either.
#pragma once

#include <google/protobuf/stubs/common.h>

#include <string>

#if GOOGLE_PROTOBUF_VERSION >= 4022000
#include <absl/strings/string_view.h>
#define PROTOBUS_PB_ERROR RecordError
#define PROTOBUS_PB_TEXT absl::string_view
#else
#define PROTOBUS_PB_ERROR AddError
#define PROTOBUS_PB_TEXT const std::string&
#endif
