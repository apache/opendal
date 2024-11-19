cmake_minimum_required(VERSION 3.10)
project(opendal-cpp-examples)

set(CMAKE_CXX_STANDARD 17)

include(FetchContent)
FetchContent_Declare(
  opendal-cpp
  GIT_REPOSITORY https://github.com/apache/opendal.git
  GIT_TAG        4b02228f7fe9a7c0f21a1660fee95716910c7a0a
  SOURCE_SUBDIR  bindings/cpp
)
FetchContent_MakeAvailable(opendal-cpp)

add_executable(basic-example basic.cpp)
target_link_libraries(basic-example opendal_cpp)
include_directories(basic-example opendal_cpp)

if(APPLE)
    target_link_libraries(basic-example "-framework CoreFoundation -framework Security")
endif()