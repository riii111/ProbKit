cmake_minimum_required(VERSION 3.20)
project(probkit-cpp VERSION 0.1.0 LANGUAGES CXX)

option(PROBKIT_BUILD_TESTS      "Build tests"               ON)
option(PROBKIT_BUILD_BENCH      "Build benchmarks"          OFF)
option(PROBKIT_ENABLE_LTO       "Enable link-time opt"      ON)
option(PROBKIT_WARNINGS_AS_ERRORS "Treat warnings as errors" ON)
option(PROBKIT_NATIVE_OPTIMIZE  "Use -march=native"         OFF)
option(PROBKIT_ENABLE_ASAN      "Enable AddressSanitizer"   OFF)
option(PROBKIT_ENABLE_UBSAN     "Enable UBSan"              OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Core library prefers no RTTI; CLI/Tests may use RTTI.

# Warnings
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wshadow -Wundef)
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  add_compile_options(-Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wshadow -Wundef)
endif()
if (PROBKIT_WARNINGS_AS_ERRORS)
  add_compile_options(-Werror)
endif()

# Native optimize
if (PROBKIT_NATIVE_OPTIMIZE AND CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  add_compile_options(-march=native)
endif()

# Sanitizers
if (PROBKIT_ENABLE_ASAN AND CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  add_link_options(-fsanitize=address)
  add_compile_options(-fsanitize=address)
endif()
if (PROBKIT_ENABLE_UBSAN AND CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  add_link_options(-fsanitize=undefined)
  add_compile_options(-fsanitize=undefined)
endif()

# LTO
include(CheckIPOSupported)
if (PROBKIT_ENABLE_LTO)
  check_ipo_supported(RESULT lto_supported OUTPUT lto_msg)
  if (lto_supported)
    message(STATUS "IPO/LTO enabled")
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)
  else()
    message(WARNING "IPO/LTO not supported: ${lto_msg}")
  endif()
endif()

# ===================== Library target =====================
add_library(probkit
  src/bloom.cpp
  src/hll.cpp
  src/cms.cpp
  src/hash.cpp
)
target_include_directories(probkit
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
# Core library: prefer no exceptions (header APIs return result<T>)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(probkit PRIVATE -fno-exceptions -fno-rtti)
endif()
# Size/speed link-time flags (gc of unused)
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(probkit PRIVATE -ffunction-sections -fdata-sections)
  target_link_options(probkit PRIVATE -Wl,--gc-sections)
endif()

# Build static lib with PIC to ease later shared linkage
set_target_properties(probkit PROPERTIES POSITION_INDEPENDENT_CODE ON)

# ===================== CLI target =====================
add_executable(probkit_cli
  cli/main.cpp
  cli/options_parse.cpp
  cli/cmd_bloom.cpp
  cli/cmd_hll.cpp
  cli/cmd_cms.cpp
)
target_link_libraries(probkit_cli PRIVATE probkit)
set_target_properties(probkit_cli PROPERTIES OUTPUT_NAME probkit)

# ===================== Tests (optional) =====================
if (PROBKIT_BUILD_TESTS)
  enable_testing()
  add_executable(probkit_tests
    tests/bloom_test.cpp
    tests/hll_test.cpp
    tests/cms_test.cpp
    tests/expected_test.cpp
    tests/hash_test.cpp
  )
  target_link_libraries(probkit_tests PRIVATE probkit)
  add_test(NAME probkit_tests COMMAND probkit_tests)
endif()

# ===================== Benchmarks (optional) =====================
if (PROBKIT_BUILD_BENCH)
  add_executable(probkit_bench
    bench/hll_bench.cpp
    bench/cms_bench.cpp
  )
  target_link_libraries(probkit_bench PRIVATE probkit)
endif()

# ===================== Install =====================
include(GNUInstallDirs)
install(TARGETS probkit probkit_cli
  EXPORT probkitTargets
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT probkitTargets
  FILE probkitTargets.cmake
  NAMESPACE probkit::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/probkit
)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/probkitConfigVersion.cmake"
  VERSION 0.1.0
  COMPATIBILITY SameMajorVersion
)
configure_file(
  ${CMAKE_CURRENT_LIST_DIR}/cmake/probkitConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/probkitConfig.cmake
  @ONLY
)

# Export compile_commands.json for tooling (clangd/clang-tidy)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Visibility defaults (reduce exposed symbols by default)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/probkitConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/probkitConfigVersion.cmake"
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/probkit
)

# Optional: enable clang-tidy as part of the build when requested
option(PROBKIT_CLANG_TIDY "Run clang-tidy during build" OFF)
if (PROBKIT_CLANG_TIDY)
  find_program(CLANG_TIDY_EXE NAMES clang-tidy)
  if (CLANG_TIDY_EXE)
    set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_EXE}")
  endif()
endif()
