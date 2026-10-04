find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

# The cross-language suite: a C++ peer, and a harness that runs it against
# the TypeScript, Python and Go ports. See crosslang/README.md.
add_library(interop_proto STATIC)
protobus_generate(TARGET interop_proto PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
target_link_libraries(interop_proto PUBLIC protobus protobuf::libprotobuf)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  target_compile_options(interop_proto PRIVATE -Wno-nullability-extension)
endif()

add_executable(cpppeer cpppeer/main.cc)
target_link_libraries(cpppeer PRIVATE interop_proto protobus)
protobus_warnings(cpppeer)

add_executable(protobus_crosslang_tests crosslang_test.cc)
target_include_directories(protobus_crosslang_tests PRIVATE ${PROJECT_SOURCE_DIR}/tests/integration)
target_link_libraries(protobus_crosslang_tests PRIVATE interop_proto protobus GTest::gtest_main)
target_compile_definitions(protobus_crosslang_tests PRIVATE
  PROTOBUS_CROSSLANG_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
  PROTOBUS_CPPPEER="$<TARGET_FILE:cpppeer>")
add_dependencies(protobus_crosslang_tests cpppeer)
protobus_warnings(protobus_crosslang_tests)
gtest_discover_tests(protobus_crosslang_tests DISCOVERY_TIMEOUT 60 PROPERTIES LABELS crosslang TIMEOUT 600)
