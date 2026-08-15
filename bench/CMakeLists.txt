cmake_minimum_required(VERSION 3.20)
project(dtq_loadgen VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Find hiredis
find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED hiredis)

# Find threads
find_package(Threads REQUIRED)

# nlohmann/json for JSON output (header-only, fetch if not found)
include(FetchContent)
FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
)
FetchContent_MakeAvailable(json)

# Load generator executable
add_executable(loadgen
    src/main.cpp
    src/producer.cpp
    src/consumer.cpp
    src/stats.cpp
    src/options.cpp
)

target_include_directories(loadgen PRIVATE
    ${HIREDIS_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(loadgen PRIVATE
    ${HIREDIS_LIBRARIES}
    Threads::Threads
    nlohmann_json::nlohmann_json
)

target_link_directories(loadgen PRIVATE
    ${HIREDIS_LIBRARY_DIRS}
)

# Install
install(TARGETS loadgen RUNTIME DESTINATION bin)
