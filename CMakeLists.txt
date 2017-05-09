cmake_minimum_required(VERSION 3.6)
project(os_ex3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")

set(SOURCE_FILES MapReduceFramework.cpp  MapReduceDerived.cpp StringContainers.cpp
        IntegerContainers.cpp ExecMap.cpp)
add_executable(os_ex3 ${SOURCE_FILES})