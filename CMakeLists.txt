cmake_minimum_required(VERSION 3.5)
project(my-proxy)

set(CMAKE_C_COMPILER "gcc")
set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-O3 -pedantic -ansi -Wno-error")
find_package(Boost REQUIRED)
include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}
)
link_directories(
        /opt/homebrew/Cellar/boost/1.79.0_1/lib
)

add_executable(tcpproxy_server tcpproxy_server.cpp acceptor.cpp bridge.cpp src/lock/Lock.h++ src/lock/detail.h++)
target_link_libraries(tcpproxy_server Boost::boost)
target_link_libraries(tcpproxy_server stdc++ pthread)
message(STATUS "arch is ${CMAKE_SYSTEM_PROCESSOR}")
IF(${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64")
    target_link_libraries(tcpproxy_server stdc++ boost_thread-mt)
ELSE(${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64")
    message(STATUS "enter")
    target_link_libraries(tcpproxy_server stdc++ boost_thread)
ENDIF(${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64")

