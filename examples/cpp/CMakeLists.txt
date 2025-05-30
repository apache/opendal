cmake_minimum_required(VERSION 3.10)
project(opendal-cpp-examples)

set(CMAKE_CXX_STANDARD 17)

# use local bindings/cpp directory
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../../bindings/cpp opendal_cpp_build)

add_executable(basic-example basic.cpp)
target_link_libraries(basic-example opendal_cpp)
include_directories(basic-example opendal_cpp)

if(APPLE)
    target_link_libraries(basic-example "-framework CoreFoundation -framework Security")
endif()