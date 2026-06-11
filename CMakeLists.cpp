cmake_minimum_required(VERSION 3.20)

project(yisync_protocol_cpp20 LANGUAGES CXX)

include(CTest)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING "" FORCE)

set(CRC32C_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CRC32C_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(CRC32C_USE_GLOG OFF CACHE BOOL "" FORCE)
set(CRC32C_INSTALL OFF CACHE BOOL "" FORCE)

add_subdirectory(crc32c EXCLUDE_FROM_ALL)

if(BUILD_TESTING AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/googletest/CMakeLists.txt")
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  add_subdirectory(googletest EXCLUDE_FROM_ALL)
endif()

add_library(yisync_protocol
  src/core/yisync_protocol.cpp
  src/core/yisync_sync.cpp
  src/network/yisync_async.cpp
  src/network/yisync_network.cpp
  src/network/yisync_scheduler.cpp
  src/receiver/yisync_commit_poller.cpp
  src/receiver/yisync_disk_writer.cpp
  src/receiver/yisync_receiver_coordinator.cpp
  src/receiver/yisync_receiver.cpp
  src/receiver/yisync_receiver_streams.cpp
  src/sender/yisync_append_state.cpp
  src/sender/yisync_chunk_resend.cpp
  src/sender/yisync_send_buffer.cpp
  src/sender/yisync_sender_plan.cpp
  src/sender/yisync_source.cpp
)

target_include_directories(yisync_protocol
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(yisync_protocol
  PUBLIC
    crc32c
)

if(APPLE)
  target_link_libraries(yisync_protocol
    PUBLIC
      "-framework CoreServices"
  )
endif()

add_executable(yisync_demo
  src/demo/main.cpp
)

target_link_libraries(yisync_demo
  PRIVATE
    yisync_protocol
)

add_executable(yisync_node
  src/node/yisync_node_common.cpp
  src/node/yisync_node.cpp
  src/receiver/yisync_receiver_app.cpp
  src/sender/yisync_sender_app.cpp
)

target_link_libraries(yisync_node
  PRIVATE
    yisync_protocol
)

if(BUILD_TESTING)
  add_executable(yisync_fault_client
    tests/cpp/fault_client.cpp
  )
  target_link_libraries(yisync_fault_client
    PRIVATE
      yisync_protocol
  )
endif()

if(BUILD_TESTING AND TARGET gtest_main)
  add_executable(yisync_unit_tests
    tests/cpp/append_state_test.cpp
    tests/cpp/chunk_resend_test.cpp
    tests/cpp/protocol_decode_test.cpp
    tests/cpp/network_scheduler_test.cpp
    tests/cpp/receiver_components_test.cpp
  )

  target_link_libraries(yisync_unit_tests
    PRIVATE
      yisync_protocol
      gtest_main
  )

  add_test(NAME yisync_unit_tests COMMAND yisync_unit_tests)
endif()

if(BUILD_TESTING)
  find_package(Python3 COMPONENTS Interpreter)
  if(Python3_Interpreter_FOUND)
    add_test(NAME yisync_integration_basic
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_ab.py
              --node $<TARGET_FILE:yisync_node>
              --scenario basic)
    add_test(NAME yisync_integration_reconnect
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_ab.py
              --node $<TARGET_FILE:yisync_node>
              --scenario reconnect)
    add_test(NAME yisync_integration_receiver_restart
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_ab.py
              --node $<TARGET_FILE:yisync_node>
              --scenario receiver-restart)
    add_test(NAME yisync_integration_entries
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_ab.py
              --node $<TARGET_FILE:yisync_node>
              --scenario entries)
    add_test(NAME yisync_integration_multistream
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_ab.py
              --node $<TARGET_FILE:yisync_node>
              --scenario multistream)
    add_test(NAME yisync_integration_limit
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_ab.py
              --node $<TARGET_FILE:yisync_node>
              --scenario limit)
    add_test(NAME yisync_integration_recovery
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_ab.py
              --node $<TARGET_FILE:yisync_node>
              --scenario recovery)
    add_test(NAME yisync_integration_final_failure
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_ab.py
              --node $<TARGET_FILE:yisync_node>
              --fault-client $<TARGET_FILE:yisync_fault_client>
              --scenario final-failure)
    add_test(NAME yisync_integration_faults
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/integration_ab.py
              --node $<TARGET_FILE:yisync_node>
              --fault-client $<TARGET_FILE:yisync_fault_client>
              --scenario faults)
  endif()
endif()
