# include_directories(${CMAKE_SOURCE_DIR}/core)
# include_directories(${CMAKE_SOURCE_DIR}/db)
# include_directories(${CMAKE_SOURCE_DIR}/lib)

add_subdirectory(core)
add_subdirectory(db)

add_executable(ycsb ycsbc.cc)
target_link_libraries(ycsb pthread glog gflags core_workload db_factory numa)
