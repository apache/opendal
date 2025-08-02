include(CMakePushCheckState)
include(CheckIncludeFileCXX)
include(CheckCXXSourceCompiles)

cmake_push_check_state()

set(CMAKE_REQUIRED_QUIET ${Coroutines_FIND_QUIETLY})

set(COROUTINES_FOUND FALSE)
set(COROUTINES_HEADER_FILE)
set(COROUTINES_NAMESPACE)
set(COROUTINES_COMPILE_FLAGS)

check_include_file_cxx("coroutine" COROUTINES_HAVE_STD_HEADER)
check_include_file_cxx("experimental/coroutine" COROUTINES_HAVE_EXPERIMENTAL_HEADER)

if(COROUTINES_HAVE_STD_HEADER)
    set(COROUTINES_HEADER_FILE "coroutine")
    set(COROUTINES_NAMESPACE "std")
elseif(COROUTINES_HAVE_EXPERIMENTAL_HEADER)
    set(COROUTINES_HEADER_FILE "experimental/coroutine")
    set(COROUTINES_NAMESPACE "std::experimental")
endif()

if(COROUTINES_HEADER_FILE)
    set(COROUTINES_TEST_SOURCE "
        #include <${COROUTINES_HEADER_FILE}>

        ${COROUTINES_NAMESPACE}::suspend_always foo() {
            co_await ${COROUTINES_NAMESPACE}::suspend_always{};
            co_return;
        }

        int main() {
            foo();
            return 0;
        }")

    check_cxx_source_compiles("${COROUTINES_TEST_SOURCE}" COROUTINES_SUPPORTED)
    
    if(NOT COROUTINES_SUPPORTED)
        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang")
            set(CMAKE_REQUIRED_FLAGS "-fcoroutines-ts")
            set(COROUTINES_FLAGS "-fcoroutines-ts")
        elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set(CMAKE_REQUIRED_FLAGS "-fcoroutines")
            set(COROUTINES_FLAGS "-fcoroutines")
        elseif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
            set(CMAKE_REQUIRED_FLAGS "/await")
            set(COROUTINES_FLAGS "/await")
        endif()
        
        check_cxx_source_compiles("${SOURCE_CODE}" COROUTINES_SUPPORTED_WITH_FLAGS)
        
        if(COROUTINES_SUPPORTED_WITH_FLAGS)
            set(COROUTINES_SUPPORTED TRUE)
        endif()
    endif()
    
    if(COROUTINES_SUPPORTED)
        set(COROUTINES_FOUND TRUE)
    endif()
endif()

cmake_pop_check_state()


set(COROUTINES_FOUND ${COROUTINES_FOUND} CACHE BOOL "Whether C++ coroutines are supported" FORCE)
set(COROUTINES_HEADER_FILE ${COROUTINES_HEADER_FILE} CACHE STRING "Coroutine header file" FORCE)
set(COROUTINES_NAMESPACE ${COROUTINES_NAMESPACE} CACHE STRING "Coroutine namespace" FORCE)
set(COROUTINES_COMPILE_FLAGS ${COROUTINES_FLAGS} CACHE STRING "Additional flags needed for coroutines" FORCE)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Coroutines
    REQUIRED_VARS 
        COROUTINES_HEADER_FILE
        COROUTINES_NAMESPACE
    HANDLE_COMPONENTS
    FAIL_MESSAGE "C++ coroutines not found. Requires C++20 compatible compiler"
)

mark_as_advanced(
    COROUTINES_FOUND
    COROUTINES_HEADER_FILE
    COROUTINES_NAMESPACE
    COROUTINES_COMPILE_FLAGS
)