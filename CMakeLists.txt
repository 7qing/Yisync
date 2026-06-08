cmake_minimum_required(VERSION 3.20)

project(yisync_protocol_cpp20 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING "" FORCE)

set(CRC32C_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CRC32C_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(CRC32C_USE_GLOG OFF CACHE BOOL "" FORCE)
set(CRC32C_INSTALL OFF CACHE BOOL "" FORCE)

add_subdirectory(crc32c EXCLUDE_FROM_ALL)

add_library(yisync_protocol
  src/yisync_async.cpp
  src/yisync_protocol.cpp
  src/yisync_receiver.cpp
  src/yisync_scheduler.cpp
  src/yisync_source.cpp
  src/yisync_sync.cpp
  src/yisync_transport.cpp
)

target_include_directories(yisync_protocol
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(yisync_protocol
  PUBLIC
    crc32c
)

add_executable(yisync_demo
  src/main.cpp
)

target_link_libraries(yisync_demo
  PRIVATE
    yisync_protocol
)

add_executable(yisync_node
  src/yisync_node_common.cpp
  src/yisync_node.cpp
  src/yisync_receiver_app.cpp
  src/yisync_sender_app.cpp
)

target_link_libraries(yisync_node
  PRIVATE
    yisync_protocol
)
