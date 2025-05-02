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

cmake_minimum_required(VERSION 3.24)
cmake_policy(SET CMP0135 NEW)

project(opendal-c)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

option(TEST_ENABLE_ASAN "Enable AddressSanitizer for tests" OFF)


option(SERVICES_AZBLOB "Enable Azblob service" OFF)
option(SERVICES_AZDLS "Enable Azdls service" OFF)
option(SERVICES_COS "Enable Cos service" OFF)
option(SERVICES_FS "Enable Fs service" OFF)
option(SERVICES_GCS "Enable Gcs service" OFF)
option(SERVICES_GHAC "Enable Ghac service" OFF)
option(SERVICES_HTTP "Enable Http service" OFF)
option(SERVICES_IPMFS "Enable Ipmfs service" OFF)
option(SERVICES_MEMORY "Enable Memory service" OFF)
option(SERVICES_OBS "Enable Obs service" OFF)
option(SERVICES_OSS "Enable Oss service" OFF)
option(SERVICES_S3 "Enable S3 service" OFF)
option(SERVICES_WEBDAV "Enable Webdav service" OFF)
option(SERVICES_WEBHDFS "Enable Webhdfs service" OFF)
option(SERVICES_AZFILE "Enable Azfile service" OFF)


# Build the list of Rust features based on enabled services
set(RUST_FEATURES "") # Initialize empty list

if(SERVICES_AZBLOB)
    list(APPEND RUST_FEATURES "services-azblob")
endif()
if(SERVICES_AZDLS)
    list(APPEND RUST_FEATURES "services-azdls")
endif()
if(SERVICES_COS)
    list(APPEND RUST_FEATURES "services-cos")
endif()
if(SERVICES_FS)
    list(APPEND RUST_FEATURES "services-fs")
endif()
if(SERVICES_GCS)
    list(APPEND RUST_FEATURES "services-gcs")
endif()
if(SERVICES_GHAC)
    list(APPEND RUST_FEATURES "services-ghac")
endif()
if(SERVICES_HTTP)
    list(APPEND RUST_FEATURES "services-http")
endif()
if(SERVICES_IPMFS)
    list(APPEND RUST_FEATURES "services-ipmfs")
endif()
if(SERVICES_MEMORY)
    list(APPEND RUST_FEATURES "services-memory")
endif()
if(SERVICES_OBS)
    list(APPEND RUST_FEATURES "services-obs")
endif()
if(SERVICES_OSS)
    list(APPEND RUST_FEATURES "services-oss")
endif()
if(SERVICES_S3)
    list(APPEND RUST_FEATURES "services-s3")
endif()
if(SERVICES_WEBDAV)
    list(APPEND RUST_FEATURES "services-webdav")
endif()
if(SERVICES_WEBHDFS)
    list(APPEND RUST_FEATURES "services-webhdfs")
endif()
if(SERVICES_AZFILE)
    list(APPEND RUST_FEATURES "services-azfile")
endif()

# Create the --features flag string for cargo
if(RUST_FEATURES)
  # Remove duplicates just in case
  list(REMOVE_DUPLICATES RUST_FEATURES)
  string(JOIN "," CARGO_FEATURES_STRING "${RUST_FEATURES}")
  set(CARGO_FEATURES_FLAG "--features=${CARGO_FEATURES_STRING}")
  message(NOTICE "-- Enabling Cargo features: ${CARGO_FEATURES_STRING}")
else()
  set(CARGO_FEATURES_FLAG "")
  message(NOTICE "-- No optional Cargo features enabled.")
endif()

# force the compiler to support these standards
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# for GoogleTest, it should be no less than C++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# fetch google test via GitHub
include(FetchContent)

set(GOOGLETEST_VERSION 1.15.2)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v${GOOGLETEST_VERSION}.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

set(CARGO_DIST_DIR "${PROJECT_SOURCE_DIR}/target/debug")
if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_BUILD_TYPE "--release")
    set(CARGO_DIST_DIR "${PROJECT_SOURCE_DIR}/target/release")
endif()

set(OPENDAL_STATIC_LIB "${CARGO_DIST_DIR}/libopendal_c${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(OPENDAL_SHARED_LIB "${CARGO_DIST_DIR}/libopendal_c${CMAKE_SHARED_LIBRARY_SUFFIX}")
message(NOTICE "-- OpenDAL C static lib: ${OPENDAL_STATIC_LIB}")
message(NOTICE "-- OpenDAL C shared lib: ${OPENDAL_SHARED_LIB}")

# custom target for cargo build
add_custom_target(cargo_build
    COMMAND sh -c "cargo build ${CARGO_BUILD_TYPE} ${CARGO_FEATURES_FLAG}"
    BYPRODUCTS  ${OPENDAL_STATIC_LIB} ${OPENDAL_SHARED_LIB}
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
)

# cmake target for static lib
add_library(opendal_c_static INTERFACE)
target_link_libraries(opendal_c_static INTERFACE ${OPENDAL_STATIC_LIB})
target_include_directories(opendal_c_static INTERFACE include)
add_dependencies(opendal_c_static cargo_build)

# cmake target for shared lib
add_library(opendal_c_shared INTERFACE)
target_link_libraries(opendal_c_shared INTERFACE ${OPENDAL_SHARED_LIB})
target_include_directories(opendal_c_shared INTERFACE include)
add_dependencies(opendal_c_shared cargo_build)

# example targets
add_executable(basic examples/basic.c)
target_link_libraries(basic opendal_c_shared)

add_executable(error_handle examples/error_handle.c)
target_link_libraries(error_handle opendal_c_shared)

# test targets
file(GLOB TEST_SRCS tests/*.cpp)
add_executable(tests ${TEST_SRCS})
target_link_libraries(tests opendal_c_shared gtest_main)
if (TEST_ENABLE_ASAN)
    target_compile_options(tests PRIVATE -fsanitize=address)
    target_link_options(tests PRIVATE -fsanitize=address)
endif()
