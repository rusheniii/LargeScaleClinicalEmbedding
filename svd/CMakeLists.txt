
cmake_minimum_required(VERSION 3.5)
project(ppmisvd)
#set(CMAKE_C_FLAGS -isysroot $ENV{CONDA_PREFIX} $ENV{CFLAGS})
add_subdirectory(src)
add_subdirectory(tests)
enable_testing()
