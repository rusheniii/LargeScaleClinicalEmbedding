
cmake_minimum_required(VERSION 3.5)
project(enumeratePairs)

find_package(Threads)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CPPFLAGS} -O3 ${OpenMP_CXX_FLAGS}")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CFLAGS} ${OpenMP_C_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${LDFLAGS}")
#include_directories($ENV{TBBROOT}/include)
include_directories($ENV{CONDA_PREFIX}/include)

#link_directories($ENV{TBBROOT}/lib/)
link_directories($ENV{CONDA_PREFIX}/lib/)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)
add_executable(orderedPairsBefore main.cpp enumeratePairs.cpp)
add_executable(orderedPairsSameDay sameDayMain.cpp enumeratePairs.cpp)

target_link_libraries(orderedPairsSameDay boost_timer)
target_link_libraries(orderedPairsSameDay boost_iostreams)
target_link_libraries(orderedPairsSameDay tbb)
target_link_libraries(orderedPairsSameDay parquet)
target_link_libraries(orderedPairsSameDay arrow)
target_link_libraries(orderedPairsSameDay ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(orderedPairsBefore boost_timer)
target_link_libraries(orderedPairsBefore boost_iostreams)
target_link_libraries(orderedPairsBefore tbb)
target_link_libraries(orderedPairsBefore parquet)
target_link_libraries(orderedPairsBefore arrow)
target_link_libraries(orderedPairsBefore ${CMAKE_THREAD_LIBS_INIT})
