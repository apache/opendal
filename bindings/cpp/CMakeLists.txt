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

cmake_minimum_required(VERSION 3.10)
project(opendal-cpp CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# cargo target dir must be absolute, otherwise some build target cannot find it
get_filename_component(CARGO_TARGET_DIR ${CMAKE_SOURCE_DIR}/../../target ABSOLUTE)
set(CARGO_MANIFEST ${CMAKE_SOURCE_DIR}/Cargo.toml)
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
target_link_libraries(opendal_cpp PUBLIC ${RUST_LIB} ${CMAKE_DL_LIBS})
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

# Documentation
set(PROJECT_DOCUMENT_SOURCE ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/README.md)
string(REPLACE ";" " " PROJECT_DOCUMENT_SOURCE "${PROJECT_DOCUMENT_SOURCE}")
file(DOWNLOAD https://cdn.jsdelivr.net/gh/jothepro/doxygen-awesome-css@2.2.1/doxygen-awesome.min.css ${CMAKE_BINARY_DIR}/doxygen-awesome.css)
find_package(Doxygen)
if (DOXYGEN_FOUND)
    set(DOXYGEN_IN ${CMAKE_SOURCE_DIR}/Doxyfile)
    set(DOXYGEN_OUT ${CMAKE_BINARY_DIR}/Doxyfile.out)

    configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
    message("Doxygen build started")

    add_custom_target(docs
        COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen"
        VERBATIM )
else (DOXYGEN_FOUND)
    message("Doxygen need to be installed to generate the doxygen documentation")
endif (DOXYGEN_FOUND)