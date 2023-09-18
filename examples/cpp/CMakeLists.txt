cmake_minimum_required(VERSION 3.10)
project(opendal-cpp-examples)

set(CMAKE_CXX_STANDARD 17)

include(FetchContent)
FetchContent_Declare(
    opendal-cpp
    URL     https://github.com/apache/incubator-opendal/releases/download/v0.40.0/opendal-cpp-v0.40.0.tar.gz
    URL_HASH SHA256=<newest-tag-sha256>
)
FetchContent_MakeAvailable(opendal-cpp)

add_executable(basic-example basic.cpp)
target_link_libraries(basic-example opendal_cpp)
include_directories(basic-example opendal_cpp)

if(APPLE)
    target_link_libraries(basic-example "-framework CoreFoundation -framework Security")
endif()