cmake_minimum_required(VERSION 3.5)

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

add_library(journal STATIC
        mmap_journal.hpp
        )

include_directories(journal ${CONAN_INCLUDE_DIRS})
target_link_libraries(journal Boost::boost)
target_link_libraries(journal stdc++ pthread)
message(STATUS "archtecture is ${CMAKE_SYSTEM_PROCESSOR}")
link_directories( ${CONAN_INCLUDE_DIRS})

IF(${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64")
    target_link_libraries(journal stdc++ boost_thread-mt)
ELSE(${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64")
    message(STATUS "enter")
    target_link_libraries(journal stdc++ boost_thread)
ENDIF(${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64")


