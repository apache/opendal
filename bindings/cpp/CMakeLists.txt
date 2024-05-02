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
project(opendal-cpp VERSION 0.45.2 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

option(OPENDAL_ENABLE_ADDRESS_SANITIZER "Enable address sanitizer" OFF)
option(OPENDAL_ENABLE_DOCUMENTATION "Enable generating document for opendal" OFF)
option(OPENDAL_DOCS_ONLY "Only build documentation (dev only for quick ci)" OFF)
option(OPENDAL_ENABLE_TESTING "Enable building test binary for opendal" OFF)
option(OPENDAL_DEV "Enable dev mode" OFF)

if (OPENDAL_DEV)
    set(OPENDAL_ENABLE_ADDRESS_SANITIZER ON)
    set(OPENDAL_ENABLE_DOCUMENTATION ON)
    set(OPENDAL_ENABLE_TESTING ON)
endif()

# Documentation
if (OPENDAL_ENABLE_DOCUMENTATION OR OPENDAL_DOCS_ONLY)
    set(PROJECT_DOCUMENT_SOURCE ${PROJECT_SOURCE_DIR}/include ${PROJECT_SOURCE_DIR}/README.md)
    string(REPLACE ";" " " PROJECT_DOCUMENT_SOURCE "${PROJECT_DOCUMENT_SOURCE}")
    file(DOWNLOAD https://cdn.jsdelivr.net/gh/jothepro/doxygen-awesome-css@2.2.1/doxygen-awesome.min.css ${CMAKE_BINARY_DIR}/doxygen-awesome.css)
    find_package(Doxygen REQUIRED)
    set(DOXYGEN_IN ${PROJECT_SOURCE_DIR}/Doxyfile)
    set(DOXYGEN_OUT ${CMAKE_BINARY_DIR}/Doxyfile.out)
    configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
    add_custom_target(docs
        COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen"
        VERBATIM)

    if (OPENDAL_DOCS_ONLY)
        return()
    endif()
endif()

# get cargo target dir using cargo locate-project
# we should this because the target dir is different for development and release
execute_process(COMMAND cargo locate-project --workspace --message-format plain
    OUTPUT_VARIABLE CARGO_TARGET_DIR
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})
string(REGEX REPLACE "/Cargo.toml\n$" "/target" CARGO_TARGET_DIR "${CARGO_TARGET_DIR}")
set(CARGO_MANIFEST ${PROJECT_SOURCE_DIR}/Cargo.toml)
set(RUST_SOURCE_FILE ${PROJECT_SOURCE_DIR}/src/lib.rs)
set(RUST_BRIDGE_CPP ${CARGO_TARGET_DIR}/cxxbridge/opendal-cpp/src/lib.rs.cc)
set(RUST_HEADER_FILE ${CARGO_TARGET_DIR}/cxxbridge/opendal-cpp/src/lib.rs.h)
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(RUST_LIB ${CARGO_TARGET_DIR}/debug/${CMAKE_STATIC_LIBRARY_PREFIX}opendal_cpp${CMAKE_STATIC_LIBRARY_SUFFIX})
else()
    set(RUST_LIB ${CARGO_TARGET_DIR}/release/${CMAKE_STATIC_LIBRARY_PREFIX}opendal_cpp${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()
set(CPP_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include ${CARGO_TARGET_DIR}/cxxbridge/opendal-cpp/src)
file(GLOB_RECURSE CPP_SOURCE_FILE src/*.cpp)
file(GLOB_RECURSE CPP_HEADER_FILE include/*.hpp)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_custom_command(
            OUTPUT ${RUST_BRIDGE_CPP} ${RUST_LIB} ${RUST_HEADER_FILE}
            COMMAND cargo build --manifest-path ${CARGO_MANIFEST}
            DEPENDS ${RUST_SOURCE_FILE}
            USES_TERMINAL
            COMMENT "Running cargo..."
    )
else()
    add_custom_command(
            OUTPUT ${RUST_BRIDGE_CPP} ${RUST_LIB} ${RUST_HEADER_FILE}
            COMMAND cargo build --manifest-path ${CARGO_MANIFEST} --release
            DEPENDS ${RUST_SOURCE_FILE}
            USES_TERMINAL
            COMMENT "Running cargo..."
    )
endif()

find_package(Boost REQUIRED COMPONENTS date_time iostreams)

add_library(opendal_cpp STATIC ${CPP_SOURCE_FILE} ${RUST_BRIDGE_CPP})
target_sources(opendal_cpp PUBLIC ${CPP_HEADER_FILE})
target_sources(opendal_cpp PRIVATE ${RUST_HEADER_FILE})
target_include_directories(opendal_cpp PUBLIC ${CPP_INCLUDE_DIR} ${Boost_INCLUDE_DIRS})
target_link_libraries(opendal_cpp PUBLIC ${RUST_LIB})
target_link_libraries(opendal_cpp PRIVATE ${CMAKE_DL_LIBS} Boost::date_time)
set_target_properties(opendal_cpp
        PROPERTIES ADDITIONAL_CLEAN_FILES ${CARGO_TARGET_DIR}
)

if (OPENDAL_ENABLE_ADDRESS_SANITIZER)
    target_compile_options(opendal_cpp PRIVATE -fsanitize=leak,address,undefined -fno-omit-frame-pointer -fno-common -O1)
    target_link_options(opendal_cpp PRIVATE -fsanitize=leak,address,undefined)
endif()

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
if (OPENDAL_ENABLE_TESTING)
    enable_testing()
    find_package(GTest REQUIRED)
    file(GLOB_RECURSE TEST_SOURCE_FILE tests/*.cpp)
    add_executable(opendal_cpp_test ${TEST_SOURCE_FILE})
    target_include_directories(opendal_cpp_test PUBLIC ${CPP_INCLUDE_DIR} ${GTEST_INCLUDE_DIRS})
    target_link_libraries(opendal_cpp_test ${GTEST_LDFLAGS} GTest::gtest_main opendal_cpp)
    target_compile_options(opendal_cpp_test PRIVATE ${GTEST_CFLAGS})

    # enable address sanitizers
    if (OPENDAL_ENABLE_ADDRESS_SANITIZER)
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
endif()
