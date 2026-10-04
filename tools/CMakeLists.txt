file(READ ${PROJECT_SOURCE_DIR}/proto/protobus/types.proto PROTOBUS_TYPES_PROTO)
configure_file(types_proto.h.in ${CMAKE_CURRENT_BINARY_DIR}/types_proto.h @ONLY)

add_library(protobus_codegen STATIC generator.cc)
target_include_directories(protobus_codegen PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(protobus_codegen PUBLIC protobuf::libprotoc protobuf::libprotobuf)
protobus_warnings(protobus_codegen)

add_executable(protoc-gen-protobus-cpp protoc-gen-protobus-cpp/main.cc)
target_link_libraries(protoc-gen-protobus-cpp PRIVATE protobus_codegen)

add_executable(protobus-cpp protobus-cpp/main.cc)
target_include_directories(protobus-cpp PRIVATE ${CMAKE_CURRENT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(protobus-cpp PRIVATE protobus_codegen protobus)
protobus_warnings(protobus-cpp)

# The generators run at build time and are not what a sanitizer build is for.
# Instrumenting them also turns on abseil's sanitizer-only checks in inline
# code that a prebuilt abseil does not match.
if(PROTOBUS_SANITIZE OR PROTOBUS_TSAN)
  foreach(tool protobus_codegen protoc-gen-protobus-cpp protobus-cpp)
    target_compile_options(${tool} PRIVATE -fno-sanitize=all)
  endforeach()
endif()

install(TARGETS protoc-gen-protobus-cpp protobus-cpp EXPORT protobusTargets RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
