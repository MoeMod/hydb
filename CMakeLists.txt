cmake_minimum_required(VERSION 3.13)
project(hydb)

# MySQL-Connecter-C++
if(WIN32)
    set(MYSQL_CONNECTOR_CPP_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/extern/mysql-connector-c++/include/jdbc")
    set(MYSQL_CONNECTOR_CPP_LIB_DIR "${CMAKE_SOURCE_DIR}/extern/mysql-connector-c++/lib/vs14")
    set(MYSQL_CONNECTOR_CPP_LIB_NAME "libeay32.lib" "ssleay32.lib" "mysqlcppconn.lib")
elseif(APPLE)
    set(MYSQL_CONNECTOR_CPP_INCLUDE_DIR "/usr/local/include")
    set(MYSQL_CONNECTOR_CPP_LIB_DIR "/usr/local/Cellar/mysql-connector-c++/1.1.9_2/lib")
    set(MYSQL_CONNECTOR_CPP_LIB_NAME "/usr/local/Cellar/mysql-connector-c++/1.1.9_2/lib/libmysqlcppconn.dylib")
endif()
add_library(mysqlcppconn INTERFACE)
target_include_directories(mysqlcppconn INTERFACE "${MYSQL_CONNECTOR_CPP_INCLUDE_DIR}")
target_link_directories(mysqlcppconn INTERFACE "${MYSQL_CONNECTOR_CPP_LIB_DIR}")
target_link_libraries(mysqlcppconn INTERFACE "${MYSQL_CONNECTOR_CPP_LIB_NAME}")

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
        )
target_include_directories(hydb PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(hydb PUBLIC mysqlcppconn xorstr)