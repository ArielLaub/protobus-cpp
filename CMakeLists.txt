cmake_minimum_required(VERSION 3.24)

project(protobus
  VERSION 2.0.0
  DESCRIPTION "RabbitMQ-native microservices for C++, with Protocol Buffers on the wire"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(PROTOBUS_BUILD_TESTS "Build the test suites" ${PROJECT_IS_TOP_LEVEL})
option(PROTOBUS_BUILD_EXAMPLES "Build the examples" ${PROJECT_IS_TOP_LEVEL})
option(PROTOBUS_BUILD_TOOLS "Build the protobus CLI and the protoc plugin" ON)
option(PROTOBUS_WERROR "Treat warnings as errors" OFF)
option(PROTOBUS_SANITIZE "Build with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(PROTOBUS_TSAN "Build with ThreadSanitizer" OFF)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

find_package(Threads REQUIRED)

# protobuf: its own CMake package where it ships one (protobuf 22 and later,
# vcpkg, Homebrew), else CMake's FindProtobuf (the distribution packages of
# protobuf 3.21, as on Debian and Ubuntu). Both provide protobuf::libprotobuf,
# protobuf::libprotoc and protobuf::protoc.
find_package(protobuf CONFIG QUIET)
if(NOT protobuf_FOUND)
  find_package(Protobuf 3.21 REQUIRED)
  set(PROTOBUS_PROTOBUF_MODULE ON)
endif()

find_package(rabbitmq-c CONFIG REQUIRED)

if(TARGET rabbitmq::rabbitmq)
  set(PROTOBUS_RABBITMQ_TARGET rabbitmq::rabbitmq)
else()
  set(PROTOBUS_RABBITMQ_TARGET rabbitmq::rabbitmq-static)
endif()

function(protobus_warnings target)
  if(MSVC)
    target_compile_options(${target} PRIVATE /W4)
  else()
    target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic -Wno-unused-parameter)
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
      # protoc's generated code uses _Nullable/_Nonnull under clang.
      target_compile_options(${target} PRIVATE -Wno-nullability-extension)
    endif()
    if(PROTOBUS_WERROR)
      target_compile_options(${target} PRIVATE -Werror)
    endif()
  endif()
endfunction()

if(PROTOBUS_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()
if(PROTOBUS_TSAN)
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
endif()

# ---- the built-in custom types ---------------------------------------------

set(PROTOBUS_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTOBUS_GENERATED_DIR}/protobus)

add_custom_command(
  OUTPUT ${PROTOBUS_GENERATED_DIR}/protobus/types.pb.cc ${PROTOBUS_GENERATED_DIR}/protobus/types.pb.h
  COMMAND protobuf::protoc
    --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
    --cpp_out=${PROTOBUS_GENERATED_DIR}
    protobus/types.proto
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/proto/protobus/types.proto protobuf::protoc
  COMMENT "Generating the protobus built-in custom types"
  VERBATIM)

# ---- the library -----------------------------------------------------------

add_library(protobus
  ${PROTOBUS_GENERATED_DIR}/protobus/types.pb.cc
  src/abort.cc
  src/base_listener.cc
  src/callback_listener.cc
  src/cancel_listener.cc
  src/config.cc
  src/connection.cc
  src/context.cc
  src/custom_types.cc
  src/errors.cc
  src/event_dispatcher.cc
  src/event_listener.cc
  src/executor.cc
  src/logger.cc
  src/message_dispatcher.cc
  src/message_factory.cc
  src/message_listener.cc
  src/message_service.cc
  src/priority.cc
  src/runnable_service.cc
  src/service_proxy.cc
  src/scheduler.cc
  src/uuid.cc
  src/amqp/memory_broker.cc
  src/amqp/rabbitmq_transport.cc
  src/amqp/transport.cc
  src/amqp/url.cc
  src/wire/envelope.cc
)
add_library(protobus::protobus ALIAS protobus)

target_include_directories(protobus
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${PROTOBUS_GENERATED_DIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src)

target_link_libraries(protobus
  PUBLIC protobuf::libprotobuf Threads::Threads
  PRIVATE ${PROTOBUS_RABBITMQ_TARGET})

target_compile_definitions(protobus PRIVATE PROTOBUS_VERSION="${PROJECT_VERSION}")
protobus_warnings(protobus)

include(cmake/ProtobusGenerate.cmake)

# ---- tools -------------------------------------------------------------------

if(PROTOBUS_BUILD_TOOLS)
  add_subdirectory(tools)
endif()

# ---- tests and examples ------------------------------------------------------

if(PROTOBUS_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
  add_subdirectory(crosslang)
endif()

if(PROTOBUS_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

# ---- install -----------------------------------------------------------------

install(TARGETS protobus EXPORT protobusTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(DIRECTORY include/protobus DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(FILES ${PROTOBUS_GENERATED_DIR}/protobus/types.pb.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/protobus)
install(FILES proto/protobus/types.proto DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/protobus)
install(FILES cmake/ProtobusGenerate.cmake DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/protobus)
install(EXPORT protobusTargets NAMESPACE protobus:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/protobus)

configure_package_config_file(cmake/protobusConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/protobusConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/protobus)
write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/protobusConfigVersion.cmake
  COMPATIBILITY SameMajorVersion)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/protobusConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/protobusConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/protobus)
