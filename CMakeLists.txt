message("\n==== zraft ====")
cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_STANDARD 20)

project(zraft)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)

add_subdirectory(simple_raft)
