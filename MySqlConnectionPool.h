#pragma once

#include <memory>
#include <string>
#include <mutex>

// https://stackoverflow.com/questions/47284705/c1z-dynamic-exception-specification-error

#if __cplusplus >= 201703L
/* MySQL override. This needed to be inclided before cppconn/exception.h to define them */
#include <stdexcept>
#include <string>
#include <memory>

/* Now remove the throw */
#define throw(...)
#include <cppconn/exception.h>
#undef throw /* reset */
#endif

#include <mysql_connection.h>
#include <mysql_driver.h>
#include <cppconn/resultset.h>
#include <vector>
#include "DatabaseConfig.h"

class MySqlConnection;

class MySqlConnectionUniqueAccessor : std::enable_shared_from_this<MySqlConnectionUniqueAccessor>
{
public:
	MySqlConnectionUniqueAccessor(std::shared_ptr<MySqlConnection> p);
	~MySqlConnectionUniqueAccessor();
	std::shared_ptr<sql::ResultSet> Query(const std::string &sql);
	int Update(const std::string &sql);

private:
	std::shared_ptr<MySqlConnection> connection;
};

class MySqlConnectionPool
{
public:
	MySqlConnectionPool(const DatabaseConfig &c = GetDatabaseConfig());
	~MySqlConnectionPool();

public:
	// ensures not nullptr
	std::shared_ptr<MySqlConnectionUniqueAccessor> acquire();

private:
	std::mutex m;
	std::vector<std::shared_ptr<MySqlConnection>> v;
	DatabaseConfig config;
};

