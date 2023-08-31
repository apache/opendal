# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.20)
project(opendal-cpp CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CARGO_MANIFEST ${CMAKE_SOURCE_DIR}/Cargo.toml)
set(CARGO_TARGET_DIR ${CMAKE_SOURCE_DIR}/../../target)
set(RUST_SOURCE_FILE ${CMAKE_SOURCE_DIR}/src/lib.rs)
set(RUST_BRIDGE_CPP ${CARGO_TARGET_DIR}/cxxbridge/opendal-cpp/src/lib.rs.cc)
set(RUST_LIB ${CARGO_TARGET_DIR}/debug/${CMAKE_STATIC_LIBRARY_PREFIX}opendal_cpp${CMAKE_STATIC_LIBRARY_SUFFIX})
set(CPP_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/include ${CARGO_TARGET_DIR}/cxxbridge/opendal-cpp/src)
file(GLOB_RECURSE CPP_SOURCE_FILE src/*.cpp)

add_custom_command(
        OUTPUT ${RUST_BRIDGE_CPP} ${RUST_LIB}
        COMMAND cargo build --manifest-path ${CARGO_MANIFEST}
        DEPENDS ${RUST_SOURCE_FILE}
        USES_TERMINAL
        COMMENT "Running cargo..."
)

add_library(opendal_cpp STATIC ${CPP_SOURCE_FILE} ${RUST_BRIDGE_CPP})
target_include_directories(opendal_cpp PUBLIC ${CPP_INCLUDE_DIR})
target_link_libraries(opendal_cpp PUBLIC ${RUST_LIB})
set_target_properties(opendal_cpp
        PROPERTIES ADDITIONAL_CLEAN_FILES ${CARGO_TARGET_DIR}
)

# Platform-specific test configuration
if(WIN32)
    target_link_libraries(opendal_cpp userenv ws2_32 bcrypt)
    set_target_properties(
            opendal_cpp
            PROPERTIES
            MSVC_RUNTIME_LIBRARY "MultiThreadedDLL"
            RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}
            RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}
    )
endif()

# Tests
enable_testing()
find_package(GTest REQUIRED)
file(GLOB_RECURSE TEST_SOURCE_FILE tests/*.cpp)
add_executable(opendal_cpp_test ${TEST_SOURCE_FILE})
target_include_directories(opendal_cpp_test PUBLIC ${CPP_INCLUDE_DIR} ${GTEST_INCLUDE_DIRS})
target_link_libraries(opendal_cpp_test ${GTEST_LDFLAGS} GTest::gtest_main opendal_cpp)
target_compile_options(opendal_cpp_test PRIVATE ${GTEST_CFLAGS})

# enable address sanitizers
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    target_compile_options(opendal_cpp_test PRIVATE -fsanitize=leak,address,undefined -fno-omit-frame-pointer -fno-common -O1)
    target_link_options(opendal_cpp_test PRIVATE -fsanitize=leak,address,undefined)
endif()

# Platform-specific test configuration
if(WIN32)
    target_link_libraries(opendal_cpp_test userenv ws2_32 bcrypt)
endif()
if(APPLE)
    target_link_libraries(opendal_cpp_test "-framework CoreFoundation -framework Security")
endif()

include(GoogleTest)
gtest_discover_tests(opendal_cpp_test)