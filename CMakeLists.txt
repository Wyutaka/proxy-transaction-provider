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
include(${CMAKE_SOURCE_DIR}/build/conanbuildinfo.cmake)
link_directories(
        /opt/homebrew/Cellar/boost/1.79.0_1/lib
)

conan_basic_setup()
#add_subdirectory(src/transaction)
#add_subdirectory(src/reqestresponse)

add_executable(tcpproxy_server
        tcpproxy_server.cpp
        query.h++
        supports.hpp
        transaction_provider.h++
        src/connector/kvs/slow.hpp
        src/journal/mmap_journal.hpp
        src/lock/detail.h++ src/lock/Lock.h++
        src/peer/Peer.hpp
        src/reqestresponse/Request.h++ src/reqestresponse/RowData.h++ src/reqestresponse/CoResponse.h++ src/transaction/transaction_impl.hpp src/transaction/sqlite3.hpp
        src/reqestresponse/Constants.h++
        src/transaction/State.h++ src/transaction/through.hpp
        acceptor.cpp bridge.cpp src/lock/Lock.h++ src/lock/detail.h++
        src/lock/shared_mutex.h++ src/test/search.h++
        src/test/search.h++
        src/test/DumpHex.h++)

add_executable(client
        src/test/client.cpp
)

include_directories(tcpproxy_server ${CONAN_INCLUDE_DIRS})
include_directories(client ${CONAN_INCLUDE_DIRS})

target_link_libraries(tcpproxy_server Boost::boost)
target_link_libraries(tcpproxy_server stdc++ pthread)
message(STATUS "archtecture is ${CMAKE_SYSTEM_PROCESSOR}")
link_directories( ${CONAN_INCLUDE_DIRS})

IF(${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64")
    target_link_libraries(tcpproxy_server stdc++ boost_thread-mt)
ELSE(${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64")
    message(STATUS "enter")
    target_link_libraries(tcpproxy_server stdc++ boost_thread)
ENDIF(${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64")

target_link_libraries(tcpproxy_server ${CONAN_LIBS})
target_link_libraries(tcpproxy_server sqlite3)
target_link_libraries(client ${CONAN_LIBS})

#target_link_libraries(tcpproxy_server transaction)

