cmake_minimum_required(VERSION 3.13)
project(hydb)

if(MSVC)
    add_compile_options(/utf-8)
endif()

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
target_include_directories(hydb PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(hydb PUBLIC xorstr)
target_link_libraries(hydb PRIVATE Boost::boost Boost::coroutine Boost::date_time)
target_link_libraries(hydb PRIVATE Boost_mysql)