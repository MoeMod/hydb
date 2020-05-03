#include "MySqlConnectionPool.h"
#include "DatabaseConfig.h"
#include "cppconn/statement.h"
#include "GlobalContext.h"
#include <boost/asio.hpp>

#include <mutex>

class MySqlConnection : std::enable_shared_from_this<MySqlConnection>
{
public:
	MySqlConnection(const DatabaseConfig &config) :
		driver(get_driver_instance()),
		config(config)
	{
		sql::ConnectOptionsMap connection_properties;
		connection_properties["hostName"] = config.url;
		connection_properties["userName"] = config.user;
		connection_properties["password"] = config.pass;
		connection_properties["OPT_RECONNECT"] = true;
		connection_properties["OPT_CHARSET_NAME"] = "utf8";

		con.reset(driver->connect(connection_properties));
		con->setSchema(config.schema);
		stmt.reset(con->createStatement());
	}

public:
	std::mutex connectMutex;
	DatabaseConfig config;
	sql::Driver *driver;
	std::unique_ptr<sql::Connection> con;
	std::unique_ptr<sql::Statement> stmt;

public:
	std::weak_ptr<MySqlConnectionUniqueAccessor> accessor;
};

void StartHeartBeat(std::weak_ptr<MySqlConnection> wconn)
{
	using namespace std::chrono_literals;
	auto ioc = GlobalContextSingleton();
	std::shared_ptr<boost::asio::system_timer> st = std::make_shared<boost::asio::system_timer>(*ioc);
	st->expires_from_now(1min);
	st->async_wait([ioc, st, wconn](const boost::system::error_code& ec) {
		if (auto conn = wconn.lock())
		{
			conn->stmt->executeQuery("SELECT 1=1;");
			StartHeartBeat(conn);
		}
	});
}

MySqlConnectionPool::MySqlConnectionPool(const DatabaseConfig & c) : config(c)
{

}

MySqlConnectionPool::~MySqlConnectionPool() = default;

std::shared_ptr<MySqlConnectionUniqueAccessor> MySqlConnectionPool::acquire()
{
	std::lock_guard l(m); // 先加锁

	std::shared_ptr<MySqlConnectionUniqueAccessor> ret = nullptr;
	while (ret == nullptr)
	{
		if (auto iter = std::find_if(v.cbegin(), v.cend(), [](const std::shared_ptr<MySqlConnection> &p) { return p->accessor.lock() == nullptr; }); iter != v.cend())
		{
			// 有可用连接，设置后返回。
			ret = std::make_shared<MySqlConnectionUniqueAccessor>(*iter);
			(*iter)->accessor = ret;
			//break;
		}
		else
		{
			auto conn = std::make_shared<MySqlConnection>(config);
			StartHeartBeat(conn);
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

MySqlConnectionUniqueAccessor::MySqlConnectionUniqueAccessor(std::shared_ptr<MySqlConnection> p) : connection(std::move(p))
{

}

MySqlConnectionUniqueAccessor::~MySqlConnectionUniqueAccessor() = default;

std::shared_ptr<sql::ResultSet> MySqlConnectionUniqueAccessor::Query(const std::string & sql)
{
	for(int iRetryTimes = 0; iRetryTimes < 3; ++iRetryTimes)
	{
		try {
			return std::shared_ptr<sql::ResultSet>(connection->stmt->executeQuery(sql));
		} catch(const sql::SQLException &e) {
			if(e.getErrorCode() == 2013) // Lost connection to MySQL server during query
				continue;
			if(e.getErrorCode() == 10060) // Can't connect to MySQL server
				break;
			throw;
		}
	}
	throw std::runtime_error("MySQL 服务器挂了");
}

int MySqlConnectionUniqueAccessor::Update(const std::string & sql)
{
	for(int iRetryTimes = 0; iRetryTimes < 3; ++iRetryTimes)
	{
		try {
			return connection->stmt->executeUpdate(sql);
		} catch(const sql::SQLException &e) {
			if(e.getErrorCode() == 2013) // Lost connection to MySQL server during query
				continue; // retry
            if (e.getErrorCode() == 10060) // Can't connect to MySQL server
                break;
			throw;
		}
	}
	throw std::runtime_error("MySQL 服务器挂了");
}
