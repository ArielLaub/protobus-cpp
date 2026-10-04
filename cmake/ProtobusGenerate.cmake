# protobus_generate(TARGET <target> PROTO_DIR <dir> [OUT_DIR <dir>] [CUSTOM_TYPES <name:wire>...])
#
# Generate C++ code for every .proto under PROTO_DIR with the protobus-cpp CLI
# (protoc's message classes plus the protobus service bases and proxies), add
# it to <target>, and put the output directory on its include path. Schemas
# are used verbatim: the custom types need no import.
function(protobus_generate)
  cmake_parse_arguments(PG "" "TARGET;PROTO_DIR;OUT_DIR" "CUSTOM_TYPES" ${ARGN})
  if(NOT PG_TARGET OR NOT PG_PROTO_DIR)
    message(FATAL_ERROR "protobus_generate needs TARGET and PROTO_DIR")
  endif()
  get_filename_component(proto_dir ${PG_PROTO_DIR} ABSOLUTE)
  if(NOT PG_OUT_DIR)
    set(PG_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/protobus_gen/${PG_TARGET})
  endif()

  if(TARGET protobus-cpp)
    set(cli protobus-cpp)
  else()
    set(cli protobus::protobus-cpp)
  endif()

  file(GLOB_RECURSE protos RELATIVE ${proto_dir} CONFIGURE_DEPENDS ${proto_dir}/*.proto)
  set(outputs)
  set(inputs)
  foreach(p ${protos})
    if(p MATCHES "^protobus/")
      continue()
    endif()
    string(REGEX REPLACE "\\.proto$" "" base ${p})
    list(APPEND inputs ${proto_dir}/${p})
    list(APPEND outputs
      ${PG_OUT_DIR}/${base}.pb.cc ${PG_OUT_DIR}/${base}.pb.h
      ${PG_OUT_DIR}/${base}.protobus.cc ${PG_OUT_DIR}/${base}.protobus.h)
  endforeach()
  set(custom_args)
  foreach(ct ${PG_CUSTOM_TYPES})
    string(REPLACE ":" ";" parts ${ct})
    list(GET parts 0 name)
    list(APPEND custom_args --custom-type ${ct})
    list(APPEND outputs
      ${PG_OUT_DIR}/protobus/custom/${name}.pb.cc ${PG_OUT_DIR}/protobus/custom/${name}.pb.h
      ${PG_OUT_DIR}/protobus/custom/${name}.protobus.cc ${PG_OUT_DIR}/protobus/custom/${name}.protobus.h)
  endforeach()

  add_custom_command(
    OUTPUT ${outputs}
    COMMAND ${cli} generate --proto-dir ${proto_dir} --out ${PG_OUT_DIR}
            --protoc $<TARGET_FILE:protobuf::protoc> ${custom_args}
    DEPENDS ${inputs} ${cli} protobuf::protoc
    COMMENT "protobus: generating C++ for ${PG_TARGET}"
    VERBATIM)
  target_sources(${PG_TARGET} PRIVATE ${outputs})
  target_include_directories(${PG_TARGET} PUBLIC ${PG_OUT_DIR})
endfunction()
