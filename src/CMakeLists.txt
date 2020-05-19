
cmake_minimum_required(VERSION 3.5)
project(enumeratePairs)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CPPFLAGS} -O3 -D_GLIBCXX_USE_CXX11_ABI=1 ${OpenMP_CXX_FLAGS}")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CFLAGS} -D_GLIBCXX_USE_CXX11_ABI=1 ${OpenMP_C_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${LDFLAGS}")
find_package(Threads)
find_package(MPI)
include_directories($ENV{CONDA_PREFIX}/include)
include_directories($ENV{PETSC_DIR}/include)
include_directories($ENV{SLEPC_DIR}/include)
include_directories($ENV{PETSC_DIR}/$ENV{PETSC_ARCH}/include/)
include_directories($ENV{SLEPC_DIR}/$ENV{PETSC_ARCH}/include/)
include_directories($ENV{SLEPC_DIR}/lib/slepc/conf/)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})

link_directories($ENV{CONDA_PREFIX}/lib/)
link_directories($ENV{PETSC_DIR}/$ENV{PETSC_ARCH}/lib/)
link_directories($ENV{SLEPC_DIR}/$ENV{PETSC_ARCH}/lib/)

add_executable(ppmisvd main.cpp ppmisvd.cpp)

target_link_libraries(ppmisvd parquet)
target_link_libraries(ppmisvd arrow)
target_link_libraries(ppmisvd blas)
target_link_libraries(ppmisvd petsc)
target_link_libraries(ppmisvd slepc)
target_link_libraries(ppmisvd ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(ppmisvd ${MPI_C_LIBRARIES})

