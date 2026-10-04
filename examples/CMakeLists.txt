# Each example generates its schema's C++ code with protobus_generate(), as a
# project using protobus would.
foreach(example calculator tokenstream combat)
  add_executable(${example} ${example}/main.cc)
  protobus_generate(TARGET ${example} PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/${example}/proto)
  target_link_libraries(${example} PRIVATE protobus)
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    target_compile_options(${example} PRIVATE -Wno-nullability-extension)
  endif()
endforeach()
