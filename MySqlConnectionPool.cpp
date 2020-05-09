#include "MySqlConnectionPool.h"
#include "DatabaseConfig.h"
#include "GlobalContext.h"
#include <boost/asio.hpp>
#include "MySqlConnection.h"

#include <mutex>

void MySqlConnectionPool::StartHeartBeat(std::weak_ptr<MySqlConnection> wconn)
{
	using namespace std::chrono_literals;
	auto ioc = GlobalContextSingleton();
	std::shared_ptr<boost::asio::system_timer> st = std::make_shared<boost::asio::system_timer>(*ioc);
	st->expires_after(1min);
	st->async_wait([ioc, st, wconn, this](const boost::system::error_code& ec) {
		if (auto conn = wconn.lock())
		{
			std::lock_guard l(this->m);
			if (conn->accessor.lock() == nullptr)
			{
				auto sp = std::make_shared<std::shared_ptr<MySqlConnection>>(conn);
				conn->accessor = sp;
				conn->connection.query("SELECT 1=1;").fetch_all();
			}
		}
	});
}

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
		if (auto iter = std::find_if(v.cbegin(), v.cend(), [](const std::shared_ptr<MySqlConnection> &p) { return p->accessor.lock() == nullptr; }); iter != v.cend())
		{
			// 有可用连接，设置后返回。
			auto sp = std::make_shared<std::shared_ptr<MySqlConnection>>(*iter);
			auto conn = *iter;
			conn->accessor = sp;
			conn->wait_for_ready();
			ret = std::shared_ptr<boost::mysql::tcp_connection>(sp, &(*sp)->connection);
			//break;
		}
		else
		{
		    auto ioc = GlobalContextSingleton();
			auto conn = std::make_shared<MySqlConnection>(config, ioc);
			conn->start();
			v.push_back(conn);
			//continue;
		}
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
