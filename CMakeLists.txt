cmake_minimum_required(VERSION 3.13)
project(hydb)

if(MSVC)
    add_compile_options(/utf-8)
endif()

set(CMAKE_CXX_STANDARD 20)

# xorstr
add_library(xorstr INTERFACE)
target_include_directories(xorstr INTERFACE ${CMAKE_SOURCE_DIR}/extern/xorstr/include)

add_library(hydb STATIC
        DatabaseConfig.cpp
        DatabaseConfig.h
        HyDatabase.cpp
        HyDatabase.h
        MySqlConnectionPool.cpp
        MySqlConnectionPool.h
        GlobalContext.cpp
        GlobalContext.h
        )

if(${CMAKE_CXX_COMPILER_ID} MATCHES "GNU")
    target_compile_options(hydb PUBLIC -fcoroutines)
endif()

target_include_directories(hydb PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(hydb PUBLIC xorstr)
target_link_libraries(hydb PUBLIC Boost::boost Boost::date_time)
target_link_libraries(hydb PUBLIC Boost::mysql OpenSSL::SSL)