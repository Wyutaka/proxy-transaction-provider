cmake_minimum_required(VERSION 3.5)
project(my-proxy)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "-O3 -pedantic -ansi -Wno-error")
find_package(Boost REQUIRED)
include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}
)

add_executable(tcpproxy_server tcpproxy_server.cpp acceptor.cpp bridge.cpp)
target_link_libraries(tcpproxy_server Boost::boost)
target_link_libraries(tcpproxy_server stdc++ pthread)

