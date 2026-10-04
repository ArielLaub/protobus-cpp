file(READ ${PROJECT_SOURCE_DIR}/proto/protobus/types.proto PROTOBUS_TYPES_PROTO)
configure_file(types_proto.h.in ${CMAKE_CURRENT_BINARY_DIR}/types_proto.h @ONLY)

add_library(protobus_codegen STATIC generator.cc)
target_include_directories(protobus_codegen PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(protobus_codegen PUBLIC protobuf::libprotoc protobuf::libprotobuf)
protobus_warnings(protobus_codegen)

add_executable(protoc-gen-protobus-cpp protoc-gen-protobus-cpp/main.cc)
target_link_libraries(protoc-gen-protobus-cpp PRIVATE protobus_codegen)

add_executable(protobus-cpp protobus-cpp/main.cc)
target_include_directories(protobus-cpp PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(protobus-cpp PRIVATE protobus_codegen protobus)
protobus_warnings(protobus-cpp)

install(TARGETS protoc-gen-protobus-cpp protobus-cpp EXPORT protobusTargets RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
