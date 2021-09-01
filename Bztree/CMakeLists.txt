set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-long-long -pedantic -fPIC -march=native")



# Set backend to PMDK by default to build persistent version. This will be passed
# in directly to PMwCAS which provides persistence support.
set(PMEM_BACKEND "PMDK" CACHE STRING "Persistent memory backend type")
string(TOUPPER ${PMEM_BACKEND} PMEM_BACKEND)

add_definitions(-DPMEM)
add_definitions(-DPMDK)

option(BUILD_TESTS "Build test cases using gflags, glog and gtest" OFF)

##################### PMwCAS #########################
set(DESC_CAP "16" CACHE STRING "Descriptor capacity")
add_definitions(-DDESC_CAP=${DESC_CAP})
add_definitions(-DPMDK_LIB_PATH=/usr/lib/x86_64-linux-gnu)
# FetchContent_Declare(PMWCAS
#   GIT_REPOSITORY https://github.com/sfu-dis/pmwcas.git
#   GIT_TAG master
#   # INSTALL_COMMAND ""
# #   BUILD_COMMAND $(MAKE)
#   # CMAKE_ARGS -DPMEM_BACKEND=${PMEM_BACKEND} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DDESC_CAP=16 -DBUILD_APPS=OFF -DGOOGLE_FRAMEWORK=OFF -DPMDK_LIB_PATH=/usr/lib/x86_64-linux-gnu
#   LOG_DOWNLOAD ON
#   LOG_CONFIGURE ON
#   LOG_BUILD ON
# )
# FetchContent_Populate(PMWCAS)
set(PMEM_BACKEND ${PMEM_BACKEND})
# set(pmwcas_DESC_CAP 16)
# set(pmwcas_BUILD_APPS OFF)
# set(pmwcas_GOOGLE_FRAMEWORK OFF)
# set(pmwcas_PMDK_LIB_PATH /usr/lib/x86_64-linux-gnu)
# #FetchContent_MakeAvailable(PMWCAS)
# add_subdirectory(${pmwcas_SOURCE_DIR} ${pmwcas_BINARY_DIR})

# PMwCAS
# include_directories(
#   ${PMWCAS_PREFIX}/src/PMWCAS
#   ${PMWCAS_PREFIX}/src/PMWCAS/src/
#   ${PMWCAS_PREFIX}/src/PMWCAS/include
# )




add_library(bztree STATIC bztree.cc
allocator_internal.cc
environment_internal.cc
environment.cc
epoch.cc
mwcas.cc
pmwcas_internal.cc
status.cc
environment_linux.cc
)
target_include_directories(bztree PRIVATE include)
####################################################

#set(LINK_FLAGS "-lnuma -lpthread -pthread -lrt")
#link_libraries(${LINK_FLAGS})


# add_dependencies(bztree pmwcas)

set(PMDK_LIB_PATH "/usr/lib/x86_64-linux-gnu" CACHE STRING "PMDK lib install path")
add_library(pmemobj SHARED IMPORTED)
set_property(TARGET pmemobj PROPERTY IMPORTED_LOCATION ${PMDK_LIB_PATH}/libpmemobj.so)

target_link_libraries(bztree numa pthread rt pmemobj)

set(MAX_FREEZE_RETRY 1 CACHE STRING "MAX retry on frozen node")
target_compile_definitions(bztree PRIVATE MAX_FREEZE_RETRY=${MAX_FREEZE_RETRY})

set(ENABLE_MERGE 0 CACHE STRING "MAX retry on frozen node")
message(STATUS "ENABLE_MERGE: " ${ENABLE_MERGE})
target_compile_definitions(bztree PRIVATE ENABLE_MERGE=${ENABLE_MERGE})
