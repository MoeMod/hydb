#pragma once

#include <memory>
#include <string>
#include <mutex>

#include <vector>
#include <boost/mysql/mysql.hpp>
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

    std::vector<boost::mysql::owning_row> query_fetch(std::string_view sql);
    std::uint64_t query_update(std::string_view sql);

	void StartHeartBeat(std::weak_ptr<MySqlConnection>);

private:
	std::mutex m;
	std::vector<std::shared_ptr<MySqlConnection>> v;
	DatabaseConfig config;
};

