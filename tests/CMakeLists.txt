find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

# The suites' schema, through the same code generation users run.
add_library(pbtest_proto STATIC)
protobus_generate(TARGET pbtest_proto PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
target_link_libraries(pbtest_proto PUBLIC protobus protobuf::libprotobuf)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  target_compile_options(pbtest_proto PRIVATE -Wno-nullability-extension)
endif()

file(GLOB unit_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/unit/*.cc)
add_executable(protobus_unit_tests ${unit_sources})
target_include_directories(protobus_unit_tests PRIVATE ${PROJECT_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(protobus_unit_tests PRIVATE pbtest_proto protobus GTest::gtest_main)
target_compile_definitions(protobus_unit_tests PRIVATE
  PROTOBUS_TEST_PROTO_DIR="${CMAKE_CURRENT_SOURCE_DIR}/proto"
  PROTOBUS_CLI="$<TARGET_FILE:protobus-cpp>"
  PROTOBUS_PROTOC="$<TARGET_FILE:protobuf::protoc>"
  PROTOBUS_SOURCE_DIR="${PROJECT_SOURCE_DIR}"
  PROTOBUS_GENERATED_DIR="${PROTOBUS_GENERATED_DIR}"
  PROTOBUS_CXX="${CMAKE_CXX_COMPILER}"
  PROTOBUS_EXTRA_INCLUDES="-I$<JOIN:$<TARGET_PROPERTY:protobuf::libprotobuf,INTERFACE_INCLUDE_DIRECTORIES>, -I>")
protobus_warnings(protobus_unit_tests)
add_dependencies(protobus_unit_tests protobus-cpp)
gtest_discover_tests(protobus_unit_tests DISCOVERY_TIMEOUT 60 PROPERTIES LABELS unit)

file(GLOB integration_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/integration/*.cc)
if(integration_sources)
  add_executable(protobus_integration_tests ${integration_sources})
  target_include_directories(protobus_integration_tests PRIVATE ${PROJECT_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR})
  target_link_libraries(protobus_integration_tests PRIVATE pbtest_proto protobus GTest::gtest GTest::gtest_main)
  protobus_warnings(protobus_integration_tests)
  gtest_discover_tests(protobus_integration_tests DISCOVERY_TIMEOUT 60 PROPERTIES LABELS integration)
endif()
