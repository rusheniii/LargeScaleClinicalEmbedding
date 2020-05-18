
cmake_minimum_required(VERSION 3.5)
project(enumeratePairs)
#set(CMAKE_C_FLAGS -isysroot $ENV{CONDA_PREFIX} $ENV{CFLAGS})
add_subdirectory(src)
add_subdirectory(test)
enable_testing()
