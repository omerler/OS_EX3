cmake_minimum_required(VERSION 3.6)
project(osEx3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# Importent line for threads useage!
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")

set(SOURCE_FILES main.cpp Search.cpp MapReduceFramework.cpp MapReduceClient.cpp)
add_executable(osEx3 ${SOURCE_FILES})