#pragma once

#include <memory>
#include <string>
#include <mutex>

#include <vector>
#include <boost/mysql.hpp>
#include "DatabaseConfig.h"

class MySqlConnection;

class MySqlConnectionPool
{
public:
	MySqlConnectionPool(const DatabaseConfig &c = GetDatabaseConfig());
	~MySqlConnectionPool();

public:
	// ensures not nullptr
	std::shared_ptr<boost::mysql::tcp_connection> acquire();
	void clear();
	void reserve(size_t n);

private:
	std::mutex m;
	std::vector<std::shared_ptr<MySqlConnection>> v;
	DatabaseConfig config;
};

