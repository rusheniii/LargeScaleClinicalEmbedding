
cmake_minimum_required(VERSION 3.5)
project(ppmisvd)
#set(CMAKE_C_FLAGS -isysroot $ENV{CONDA_PREFIX} $ENV{CFLAGS})
add_subdirectory(svd)
add_subdirectory(cooccurrenceMatrix)
enable_testing()
