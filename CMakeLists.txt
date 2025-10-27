cmake_minimum_required(VERSION 3.27)
cmake_policy(SET CMP0144 NEW)
project(upq)

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
include_directories(${upq_SOURCE_DIR})

FetchContent_Declare(
        uvent
        GIT_REPOSITORY https://github.com/Usub-development/uvent.git
        GIT_TAG main
        OVERRIDE_FIND_PACKAGE
)

FetchContent_MakeAvailable(uvent)

add_executable(${PROJECT_NAME}
        examples/main.cpp
        include/upq/PgTypes.h
        include/upq/PgConnection.h
        include/upq/PgPool.h
        src/upq/PgConnection.cpp
        src/upq/PgPool.cpp
        src/upq/PgTypes.cpp
        include/upq/PgTransaction.h
        src/upq/PgTransaction.cpp
)

target_include_directories(${PROJECT_NAME} PRIVATE
        /usr/local/include
        ${CMAKE_CURRENT_LIST_DIR}/include
)

target_link_libraries(${PROJECT_NAME} PRIVATE
        usub::uvent
        OpenSSL::Crypto
        -lpq
        PostgreSQL::PostgreSQL
)
