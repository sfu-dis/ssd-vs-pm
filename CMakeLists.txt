cmake_minimum_required(VERSION 3.10)

# set the project name
project(optane-storage)

# set default build type
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "" FORCE)
  message("-- No build mode chosen - using Debug by default")
endif(NOT CMAKE_BUILD_TYPE)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 17)

# Compile options enabled for all builds
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DALIGNMENT=512 -DPAGE_SIZE=4096 -fPIC")

if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g2 -Wall -Wextra -Wno-unused-parameter -fno-omit-frame-pointer -march=native -mtune=native")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -march=native -mtune=native")
endif()

# add options
OPTION(IOSTAT "Count IOPS" OFF) # Disabled by default
IF(IOSTAT)
  add_definitions(-DIOSTAT)
ENDIF(IOSTAT)

OPTION(CLUSTERED "Only build the index tree" ON) # Enabled by default
IF(CLUSTERED)
  add_definitions(-DCLUSTERED)
ENDIF(CLUSTERED)

OPTION(NO_BUFFER "Disable the buffer manager" OFF) # Keep the buffer manager by default
IF(NO_BUFFER)
  add_definitions(-DNO_BUFFER)
ENDIF(NO_BUFFER)

OPTION(VERIFY_VALUE "Verify values" OFF) 
IF(VERIFY_VALUE)
  add_definitions(-DVERIFY_VALUE)
ENDIF(VERIFY_VALUE)


add_subdirectory(include)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
# include_directories(${CMAKE_SOURCE_DIR})
add_subdirectory(Bztree)
add_subdirectory(abseil-cpp)
include_directories(Btree)
add_subdirectory(Btree)
add_subdirectory(HashTable)

add_subdirectory(Ycsb)
