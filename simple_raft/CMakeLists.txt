message("\n==== simple_raft ====")

add_executable(simple_raft main.cpp raft_server.cpp)
target_include_directories(simple_raft PRIVATE ${PROJECT_SOURCE_DIR})
target_link_libraries(simple_raft pthread)