
cmake_minimum_required(VERSION 3.5)
project(enumeratePairs)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CPPFLAGS} -O3 ${OpenMP_CXX_FLAGS}")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CFLAGS} ${OpenMP_C_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${LDFLAGS}")
find_package(Threads)
#include_directories($ENV{TBBROOT}/include)
include_directories($ENV{CONDA_PREFIX}/include)

#link_directories($ENV{TBBROOT}/lib/)
link_directories($ENV{CONDA_PREFIX}/lib/)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)
add_executable(enumeratePairs main.cpp enumeratePairs.cpp)

target_link_libraries(enumeratePairs boost_timer)
target_link_libraries(enumeratePairs boost_iostreams)
target_link_libraries(enumeratePairs tbb)
target_link_libraries(enumeratePairs parquet)
target_link_libraries(enumeratePairs arrow)
target_link_libraries(enumeratePairs ${CMAKE_THREAD_LIBS_INIT})
