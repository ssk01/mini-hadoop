cmake_minimum_required(VERSION 3.20)
project(mini-hadoop VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BUILD_TESTS "Build unit tests" ON)

if(APPLE)
  add_definitions(-DMINI_HADOOP_USE_COMMON_CRYPTO)
endif()
if(NOT APPLE)
  find_package(OpenSSL REQUIRED)
endif()

include(FetchContent)

FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG        v1.15.2
)
FetchContent_MakeAvailable(spdlog)

FetchContent_Declare(
  tomlplusplus
  GIT_REPOSITORY https://github.com/marzer/tomlplusplus.git
  GIT_TAG        v3.4.0
)
FetchContent_MakeAvailable(tomlplusplus)

if(BUILD_TESTS)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.16.0
  )
  FetchContent_MakeAvailable(googletest)
  enable_testing()
endif()

add_subdirectory(src)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()
