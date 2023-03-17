cmake_minimum_required(VERSION 3.1)

project(OPENDAL)

find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIR})
include_directories(include)
#find_package(Threads REQUIRED)

add_executable(runUnitTests tests/hello_test.cpp)
get_test_property(${PROJECT_NAME} WORKING_DIRECTORY test_dir)

target_link_libraries(runUnitTests ${GTEST_BOTH_LIBRARIES} ${CMAKE_SOURCE_DIR}/../../target/debug/libopendal.so)

add_test(Test runUnitTests)
enable_testing()
