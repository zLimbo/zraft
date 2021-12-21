message("\n==== zraft ====")
cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_STANDARD 17)

project(zraft)

add_executable(server simple_raft/server.cpp)
target_include_directories(server PRIVATE .)
target_link_libraries(server pthread)