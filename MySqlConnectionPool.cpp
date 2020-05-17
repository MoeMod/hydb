#include "MySqlConnectionPool.h"
#include "DatabaseConfig.h"
#include "GlobalContext.h"
#include <boost/asio.hpp>
#include "MySqlConnection.h"

#include <mutex>

MySqlConnectionPool::MySqlConnectionPool(const DatabaseConfig & c) : config(c)
{

}

MySqlConnectionPool::~MySqlConnectionPool() = default;

std::shared_ptr<boost::mysql::tcp_connection> MySqlConnectionPool::acquire()
{
	std::lock_guard l(m); // 先加锁

	std::shared_ptr<boost::mysql::tcp_connection> ret = nullptr;
	while (ret == nullptr)
	{
		if (auto iter = std::find_if(v.cbegin(), v.cend(), [](const std::shared_ptr<MySqlConnection> &p) { return p->status.load() == MySqlConnection::Status::available; }); iter != v.cend())
		{
			// 有可用连接，设置后返回。
			auto conn = *iter;
			auto expected = MySqlConnection::Status::available;
			if (conn->status.compare_exchange_strong(expected, MySqlConnection::Status::in_use))
			{
				std::shared_ptr<MySqlConnection> sp(conn.get(), [](MySqlConnection* p) { 
					assert(p->status.load() == MySqlConnection::Status::in_use);
					p->status.store(MySqlConnection::Status::available); 
				});
				ret = std::shared_ptr<boost::mysql::tcp_connection>(sp, &conn->connection);
				break;
			}
		}

		auto ioc = GlobalContextSingleton();
		auto conn = std::make_shared<MySqlConnection>(config, ioc);
		conn->start();
		v.push_back(conn);
		std::this_thread::yield();
		//continue;
	}
	return ret;
}

void MySqlConnectionPool::clear()
{
	std::lock_guard l(m); // 先加锁
	v.clear();
}

std::vector<boost::mysql::owning_row> MySqlConnectionPool::query_fetch(std::string_view sql)
{
    auto conn = acquire();
    return conn->query(sql).fetch_all();
}

std::uint64_t MySqlConnectionPool::query_update(std::string_view sql)
{
    auto conn = acquire();
	return conn->query(sql).affected_rows();
}
