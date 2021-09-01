add_library(db_factory db_factory.cc)
add_library(db_btree db_btree.cc)
add_library(db_hashtable db_hashtable.cc)
add_library(db_pibench db_pibench.cc)
add_library(db_dash db_dash.cc)
add_library(db_bztree db_bztree.cc)
target_link_libraries(db_btree buffer_manager glog gflags)
target_link_libraries(db_hashtable glog gflags absl::flat_hash_map)
target_link_libraries(db_pibench ${CMAKE_DL_LIBS})
target_link_libraries(db_dash pmemobj pmem pthread gflags)
target_link_libraries(db_bztree bztree)
target_link_libraries(db_factory db_btree db_pibench db_dash db_hashtable db_bztree glog gflags)

target_compile_definitions(db_bztree PUBLIC -DPMDK)

include(FetchContent)
set(dash_USE_PMEM ON)
add_definitions(-DPMEM)
set(epoch_reclaimer_PMEM ON)

FetchContent_Declare(
    epoch_reclaimer
    GIT_REPOSITORY https://github.com/XiangpengHao/epoch-reclaimer.git
    GIT_TAG master
)
FetchContent_Populate(epoch_reclaimer)

FetchContent_Declare(
    dash
    GIT_REPOSITORY git@github.com:baotonglu/dash.git
)
FetchContent_Populate(dash)

target_include_directories(db_dash 
    PRIVATE ${epoch_reclaimer_SOURCE_DIR}
    PRIVATE ${dash_SOURCE_DIR}/src
)

# from https://github.com/hpides/viper/blob/2780f547a57aac4f7401b2e0a6c4ca47eec9b243/benchmark/CMakeLists.txt#L148
execute_process(COMMAND sed -i -e "s/, (void\\*)pool_addr//g" ${dash_SOURCE_DIR}/src/allocator.h)
execute_process(COMMAND sed -i -e "s/pmemobj_create_addr/pmemobj_create/g" ${dash_SOURCE_DIR}/src/allocator.h)
execute_process(COMMAND sed -i -e "s/pmemobj_open_addr/pmemobj_open/g" ${dash_SOURCE_DIR}/src/allocator.h)
execute_process(COMMAND sed -i -e "s#std::cout << \"Directory_Doubling#//#g" ${dash_SOURCE_DIR}/src/ex_finger.h)
