cmake_minimum_required(VERSION 3.27)
cmake_policy(SET CMP0144 NEW)
project(upq VERSION 0.1.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0" CACHE STRING "Debug flags" FORCE)
set(CMAKE_C_FLAGS_DEBUG "-g -O0" CACHE STRING "Debug flags" FORCE)

add_compile_definitions(DEV_STAGE=${DEV_STAGE})

message(STATUS "DEV_STAGE is set to ${DEV_STAGE}")

find_package(OpenSSL REQUIRED)
find_package(PostgreSQL REQUIRED)

include(FetchContent)

FetchContent_Declare(
        uvent
        GIT_REPOSITORY https://github.com/Usub-development/uvent.git
        GIT_TAG main
        OVERRIDE_FIND_PACKAGE
)

FetchContent_Declare(
        ureflect
        GIT_REPOSITORY https://github.com/Usub-development/ureflect.git
        GIT_TAG main
        OVERRIDE_FIND_PACKAGE
)

FetchContent_MakeAvailable(uvent)
FetchContent_MakeAvailable(ureflect)

file(GLOB_RECURSE UPQ_HEADERS CONFIGURE_DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/include/upq/*.h
        ${CMAKE_CURRENT_SOURCE_DIR}/include/upq/*.hpp
)
file(GLOB_RECURSE UPQ_SOURCES CONFIGURE_DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/src/upq/*.cpp
)

add_library(upq ${UPQ_SOURCES} ${UPQ_HEADERS}
        src/upq/PgRoutingBuilder.cpp
        include/upq/PgRoutingTxAdapter.h)
add_library(usub::upq ALIAS upq)

target_include_directories(upq
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/upq>
)

target_link_libraries(upq
        PUBLIC
        usub::uvent
        usub::ureflect
        OpenSSL::Crypto
        PostgreSQL::PostgreSQL
)

if (TARGET PostgreSQL::pq)
        target_link_libraries(upq PUBLIC PostgreSQL::pq)
endif()

set_target_properties(upq PROPERTIES
        EXPORT_NAME upq
        VERSION ${PROJECT_VERSION}
        SOVERSION ${PROJECT_VERSION_MAJOR}
)

option(UPQ_BUILD_EXAMPLES "Build UPQ example executables" ON)
if (UPQ_BUILD_EXAMPLES)
        add_executable(upq_example examples/main.cpp)
        target_link_libraries(upq_example PRIVATE upq)
        target_compile_definitions(upq_example PRIVATE DEV_STAGE=${DEV_STAGE})
endif()

install(TARGETS upq
        EXPORT upqTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/upq
)

install(DIRECTORY include/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/upq
        FILES_MATCHING
        PATTERN "*.h"
        PATTERN "*.hpp"
)

configure_package_config_file(
        cmake/upqConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/upqConfig.cmake
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/upq
        NO_CHECK_REQUIRED_COMPONENTS_MACRO
)

write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/upqConfigVersion.cmake
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
)

install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/upqConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/upqConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/upq
)

install(EXPORT upqTargets
        NAMESPACE upq::
        FILE upqTargets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/upq
)
