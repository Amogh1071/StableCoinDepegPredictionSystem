cmake_minimum_required(VERSION 3.16)
project(depegpred)

set(CMAKE_CXX_STANDARD 17)

add_executable(depegpred main.cpp
        main1.cpp)
add_subdirectory(libs/cpr)
# Add include directories
target_include_directories(depegpred PRIVATE
        ${CMAKE_SOURCE_DIR}/libs/rapidjson/include
)
add_library(sqlite3 STATIC libs/sqlite/sqlite3.c)
target_include_directories(depegpred PRIVATE libs/sqlite)
target_link_libraries(depegpred PRIVATE cpr::cpr sqlite3 pthread)
# Add library search path
target_link_libraries(depegpred PRIVATE cpr::cpr)
