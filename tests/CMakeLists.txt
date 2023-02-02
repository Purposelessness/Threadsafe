cmake_minimum_required(VERSION 3.24)
project(ThreadsafeTests C CXX)

set(CMAKE_CXX_STANDARD 20)

find_package(GTest REQUIRED)

add_executable(test_lib test_lib.cc)
target_link_libraries(test_lib 
  ${GTEST_LIBRARIES})

