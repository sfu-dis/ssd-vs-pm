add_library(buffer_manager buffer_manager.cc)
target_link_libraries(buffer_manager absl::flat_hash_map)
