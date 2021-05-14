#include "HyDatabase.h"
#include "MySqlConnectionPool.h"

#include <random>
#include <atomic>
#include <string>
#include <future>
#include <string_view>
#include <numeric>
#include <assert.h>

#include "GlobalContext.h"
#include <boost/asio.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "VariantVisitor.h"

struct CHyDatabase::impl_t
{
public:
    std::shared_ptr<boost::asio::io_context> ioc = GlobalContextSingleton();
	MySqlConnectionPool pool;
};

CHyDatabase CHyDatabase::instance;
CHyDatabase &HyDatabase()
{
	return CHyDatabase::instance;
}

CHyDatabase::CHyDatabase() : pimpl(std::make_shared<impl_t>()) {}

CHyDatabase::~CHyDatabase() = default;

// qqid, name, steamid, xscode, access, tag
static HyUserAccountData UserAccountDataFromSqlResult(const std::vector<boost::mysql::row> &res)
{
	if (res.empty())
		throw InvalidUserAccountDataException();
	const auto &line = res[0].values();
	return HyUserAccountData{
            visit(IntegerVisitor<int64_t>(), line[0].to_variant()),
            visit(StringVisitor(), line[1].to_variant()),
            visit(StringVisitor(), line[2].to_variant()),
            visit(IntegerVisitor<int32_t>(), line[3].to_variant()),
            visit(StringVisitor(), line[4].to_variant()),
            visit(StringVisitor(), line[5].to_variant())
	};
}

HyUserAccountData CHyDatabase::QueryUserAccountDataByQQID(int64_t fromQQ)
{
	return UserAccountDataFromSqlResult(pimpl->pool.acquire()->query(
		"SELECT qqid, name, steamid, xscode, access, tag FROM qqlogin "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS qqid, uid FROM idlink WHERE idsrc = 'qq') AS T1 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS name, uid FROM idlink WHERE idsrc = 'name') AS T2 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS steamid, uid FROM idlink WHERE idsrc = 'steam') AS T3 "
		"WHERE `qqid` = '" + std::to_string(fromQQ) + "';"
	).read_all());
}

boost::asio::awaitable<HyUserAccountData> CHyDatabase::async_QueryUserAccountDataByQQID(int64_t fromQQ)
{
	auto sql = std::string(
		"SELECT qqid, name, steamid, xscode, access, tag FROM qqlogin "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS qqid, uid FROM idlink WHERE idsrc = 'qq') AS T1 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS name, uid FROM idlink WHERE idsrc = 'name') AS T2 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS steamid, uid FROM idlink WHERE idsrc = 'steam') AS T3 "
		"WHERE `qqid` = '" + std::to_string(fromQQ) + "';"
	);
	auto conn = pimpl->pool.acquire();
    boost::mysql::tcp_resultset resultset = co_await conn->async_query(sql, boost::asio::use_awaitable);

    auto res = co_await resultset.async_read_all(boost::asio::use_awaitable);
    co_return UserAccountDataFromSqlResult(res);
}

HyUserAccountData CHyDatabase::QueryUserAccountDataBySteamID(const std::string& steamid) noexcept(false)
{
	return UserAccountDataFromSqlResult(pimpl->pool.acquire()->query(
		"SELECT qqid, name, steamid, xscode, access, tag FROM qqlogin "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS qqid, uid FROM idlink WHERE idsrc = 'qq') AS T1 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS name, uid FROM idlink WHERE idsrc = 'name') AS T2 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS steamid, uid FROM idlink WHERE idsrc = 'steam') AS T3 "
		"WHERE `steamid` = '" + steamid + "';"
	).read_all());
}

boost::asio::awaitable<HyUserAccountData> CHyDatabase::async_QueryUserAccountDataBySteamID(const std::string &steamid)
{
	auto sql = std::string(
		"SELECT qqid, name, steamid, xscode, access, tag FROM qqlogin "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS qqid, uid FROM idlink WHERE idsrc = 'qq') AS T1 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS name, uid FROM idlink WHERE idsrc = 'name') AS T2 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS steamid, uid FROM idlink WHERE idsrc = 'steam') AS T3 "
		"WHERE `steamid` = '" + steamid + "';"
		);
    auto conn = pimpl->pool.acquire();
    boost::mysql::tcp_resultset resultset = co_await conn->async_query(sql, boost::asio::use_awaitable);
    auto res = co_await resultset.async_read_all(boost::asio::use_awaitable);
    co_return UserAccountDataFromSqlResult(res);
}

bool CHyDatabase::UpdateXSCodeByQQID(int64_t qqid, int32_t xscode)
{
	auto res1 = pimpl->pool.acquire()->query("UPDATE qqlogin SET `xscode` = '" + std::to_string(xscode) + "' WHERE `qqid` = '" + std::to_string(qqid) + "';").affected_rows();
	return res1 == 1;
}

bool CHyDatabase::BindQQToCS16Name(int64_t new_qqid, int32_t xscode)
{
    auto conn = pimpl->pool.acquire();
	auto res1 = conn->query("SELECT `name` FROM cs16reg WHERE `xscode` = '" + std::to_string(xscode) + "';").read_all();
	if (res1.empty())
		return false;
	
	const std::string name = visit(StringVisitor(), res1[0].values()[0].to_variant());
	int uid = 0;
	while (1)
	{
		auto res2 = conn->query("SELECT `uid` FROM idlink WHERE `idsrc` = 'qq' AND `auth` = '" + std::to_string(new_qqid) + "';").read_all();
		if (res2.empty())
		{
			//没有注册过，插入新的uid
            conn->query("INSERT IGNORE INTO idlink(idsrc, auth) VALUES('qq', '" + std::to_string(new_qqid) + "');");
            conn->query("INSERT IGNORE INTO qqlogin(qqid) VALUES('" + std::to_string(new_qqid) + "');");
			continue;
		}
		//已经注册过，得到原先的uid 
		uid = visit(IntegerVisitor<int>(), res2[0].values()[0].to_variant());
		break;
	}
	//用uid和steamid注册
	auto res3 = conn->query("INSERT IGNORE INTO idlink(idsrc, auth, uid) VALUES('name', '" + name + "', '" + std::to_string(uid) + "');").affected_rows();
	//删掉cs16reg里面的表项，不管成不成功都无所谓了
    conn->query("DELETE FROM cs16reg WHERE `name` = '" + name + "';");
	return res3 == 1;
}

bool CHyDatabase::BindQQToSteamID(int64_t new_qqid, int32_t gocode)
{
    auto conn = pimpl->pool.acquire();
	auto res1 = conn->query("SELECT `steamid` FROM csgoreg WHERE `gocode` = '" + std::to_string(gocode) + "';").read_all();
	if (res1.empty())
		return false; // 没有记录的注册id

	const std::string steamid = visit(StringVisitor(), res1[0].values()[0].to_variant());
	int uid = 0; 
	while (1)
	{
		auto res2 = conn->query("SELECT `uid` FROM idlink WHERE `idsrc` = 'qq' AND `auth` = '" + std::to_string(new_qqid) + "';").read_all();
		if (res2.empty())
		{
			//没有注册过，插入新的uid
            conn->query("INSERT IGNORE INTO idlink(idsrc, auth) VALUES('qq', '" + std::to_string(new_qqid) + "');").read_all();
            conn->query("INSERT IGNORE INTO qqlogin(qqid) VALUES('" + std::to_string(new_qqid) + "');").read_all();
			continue;
		}
		//已经注册过，得到原先的uid 
		uid = visit(IntegerVisitor<int>(), res2[0].values()[0].to_variant());
		break;
	}
	//用uid和steamid注册
	auto res3 = conn->query("INSERT IGNORE INTO idlink(idsrc, auth, uid) VALUES('steam', '" + steamid + "', '" + std::to_string(uid) + "');").affected_rows();
	//删掉csgoreg里面的表项，不管成不成功都无所谓了
    conn->query("DELETE FROM csgoreg WHERE `steamid` = '" + steamid + "';");
	return res3 == 1;
}

boost::asio::awaitable<int32_t> CHyDatabase::async_StartRegistrationWithSteamID(const std::string& steamid)
{
	auto ioc = pimpl->ioc;
	auto conn = pimpl->pool.acquire();

    static std::random_device rd;
    std::string steamid_hash = std::to_string(std::hash<std::string>()(steamid));
    std::string gocode(8, '0');
    while(steamid_hash.size() < gocode.size())
        steamid_hash.push_back(std::uniform_int_distribution<int>('0', '9')(rd));
    int iMaxTries = 10;
    do {
        if (--iMaxTries == 0)
            co_return 0;
        co_await conn->async_query("DELETE FROM csgoreg WHERE `steamid` = '" + steamid + "';", boost::asio::use_awaitable);
        std::sample(steamid_hash.begin(), steamid_hash.end(), gocode.begin(), gocode.size(), std::mt19937(rd()));
    } while ((co_await conn->async_query("INSERT IGNORE INTO csgoreg(steamid, gocode) VALUES('" + steamid + "', '" + gocode+ "');", boost::asio::use_awaitable)).affected_rows() != 1);
    co_return std::stoi(gocode);
}

// `code`, `name`, `desc`, `quantifier`
static HyItemInfo HyItemInfoFromSqlLine(const std::vector<boost::mysql::value> &line)
{
	return HyItemInfo{
            visit(StringVisitor(), line[0].to_variant()),
            visit(StringVisitor(), line[1].to_variant()),
            visit(StringVisitor(), line[2].to_variant()),
            visit(StringVisitor(), line[3].to_variant())
	};
}

static std::vector<HyItemInfo> InfoListFromSqlResult(const std::vector<boost::mysql::row> & res)
{
	std::vector<HyItemInfo> result;
	for(auto &l : res)
	{
		result.push_back(HyItemInfoFromSqlLine(l.values()));
	}
	return result;
}

// `code`, `name`, `desc`, `quantifier`, `amount`
static std::vector<HyUserOwnItemInfo> UserOwnItemInfoListFromSqlResult(const std::vector<boost::mysql::row> &res)
{
    std::vector<HyUserOwnItemInfo> result;
	for(auto &l : res)
	{
		result.push_back({ HyItemInfoFromSqlLine(l.values()), visit(IntegerVisitor<int>(), l.values()[4].to_variant()) });
	}
    return result;
}

std::vector<HyItemInfo> CHyDatabase::AllItemInfoAvailable() noexcept(false)
{
	return InfoListFromSqlResult(pimpl->pool.acquire()->query(
		"SELECT `code`, `name`, `desc`, `quantifier` FROM iteminfo;"
	).read_all());
}

boost::asio::awaitable<std::vector<HyItemInfo>> CHyDatabase::async_AllItemInfoAvailable()
{
	auto conn = pimpl->pool.acquire();
	boost::mysql::tcp_resultset resultset = co_await conn->async_query("SELECT `code`, `name`, `desc`, `quantifier` FROM iteminfo;", boost::asio::use_awaitable);
	std::vector<boost::mysql::row> res = co_await resultset.async_read_all(boost::asio::use_awaitable);

	co_return InfoListFromSqlResult(res);
}

std::vector<HyUserOwnItemInfo> CHyDatabase::QueryUserOwnItemInfoByQQID(int64_t qqid)
{
	return UserOwnItemInfoListFromSqlResult(pimpl->pool.acquire()->query(
		"SELECT `code`, `name`, `desc`, `quantifier`, `amount` FROM iteminfo NATURAL JOIN ("
			"SELECT code, CAST(SUM(amount) AS SIGNED INTEGER) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
			"WHERE idl2.idsrc = 'qq' AND idl2.auth = '" + std::to_string(qqid) + "' UNION (SELECT 'qq', '" + std::to_string(qqid) + "') ) AS idl GROUP BY code "
		") AS itemlst;"
	).read_all());
}

boost::asio::awaitable<std::vector<HyUserOwnItemInfo>> CHyDatabase::async_QueryUserOwnItemInfoByQQID(int64_t qqid)
{
	auto conn = pimpl->pool.acquire();
    std::string sql(
			"SELECT `code`, `name`, `desc`, `quantifier`, `amount` FROM iteminfo NATURAL JOIN ("
			"SELECT code, CAST(SUM(amount) AS SIGNED INTEGER) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
			"WHERE idl2.idsrc = 'qq' AND idl2.auth = '" + std::to_string(qqid) + "' UNION (SELECT 'qq', '" + std::to_string(qqid) + "') ) AS idl GROUP BY code "
			") AS itemlst;"
		);
    boost::mysql::tcp_resultset resultset = co_await conn->async_query(sql, boost::asio::use_awaitable);
    std::vector<boost::mysql::row> res = co_await resultset.async_read_all(boost::asio::use_awaitable);
    co_return UserOwnItemInfoListFromSqlResult(std::move(res));
}

std::vector<HyUserOwnItemInfo> CHyDatabase::QueryUserOwnItemInfoBySteamID(const std::string &steamid) noexcept(false)
{
	return UserOwnItemInfoListFromSqlResult(pimpl->pool.acquire()->query(
			"SELECT `code`, `name`, `desc`, `quantifier`, `amount` FROM iteminfo NATURAL JOIN ("
			"SELECT code, CAST(SUM(amount) AS SIGNED INTEGER) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
			"WHERE idl2.idsrc = 'steam' AND idl2.auth = '" + steamid + "' UNION (SELECT 'steam', '" + steamid + "') ) AS idl GROUP BY code "
			") AS itemlst;"
	).read_all());
}

boost::asio::awaitable<std::vector<HyUserOwnItemInfo>> CHyDatabase::async_QueryUserOwnItemInfoBySteamID(const std::string &steamid)
{
	auto conn = pimpl->pool.acquire();
    std::string sql(
			"SELECT `code`, `name`, `desc`, `quantifier`, `amount` FROM iteminfo NATURAL JOIN ("
			"SELECT code, CAST(SUM(amount) AS SIGNED INTEGER) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
			"WHERE idl2.idsrc = 'steam' AND idl2.auth = '" + steamid + "' UNION (SELECT 'steam', '" + steamid + "') ) AS idl GROUP BY code "
			") AS itemlst;"
	);
    boost::mysql::tcp_resultset resultset = co_await conn->async_query(sql, boost::asio::use_awaitable);
    std::vector<boost::mysql::row> res = co_await resultset.async_read_all(boost::asio::use_awaitable);
    co_return UserOwnItemInfoListFromSqlResult(std::move(res));
}

int32_t CHyDatabase::GetItemAmountByQQID(int64_t qqid, const std::string &code) noexcept(false)
{
	auto res = pimpl->pool.acquire()->query(
			"SELECT CAST(SUM(amount) AS SIGNED INTEGER) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
			"WHERE idl2.idsrc = 'qq' AND idl2.auth = '" + std::to_string(qqid) + "' UNION (SELECT 'qq', '" + std::to_string(qqid) + "') ) AS idl WHERE `code` = '" + code + "';"
	).read_all();

	if (!res.empty()){
		return visit(IntegerVisitor<int32_t>(), res[0].values()[0].to_variant());
	}
	return 0;
}

void CHyDatabase::async_GetItemAmountByQQID(int64_t qqid, const std::string& code, std::function<void(int32_t)> fn)
{
	auto conn = pimpl->pool.acquire();
	auto sql = std::make_shared<std::string>(
		"SELECT CAST(SUM(amount) AS SIGNED INTEGER) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
		"WHERE idl2.idsrc = 'steam' AND idl2.auth = '" + std::to_string(qqid) + "' UNION (SELECT 'qq', '" + std::to_string(qqid) + "') ) AS idl WHERE `code` = '" + code + "';"
		);
	conn->async_query(*sql, [fn, conn, sql](boost::system::error_code ec, boost::mysql::tcp_resultset&& resultset) {
		if (ec || !resultset.valid())
			return fn(0);
		auto resultset_keep = std::make_shared<boost::mysql::tcp_resultset>(std::move(resultset));
		resultset_keep->async_read_all([fn, conn, resultset_keep](boost::system::error_code ec, std::vector<boost::mysql::row> res) {
			if (ec)
				return fn(0);
			return fn(!res.empty() ? visit(IntegerVisitor<int32_t>(), res[0].values()[0].to_variant()) : 0);
			});
		});
}

int32_t CHyDatabase::GetItemAmountBySteamID(const std::string &steamid, const std::string & code) noexcept(false)
{
	auto res = pimpl->pool.acquire()->query(
			"SELECT CAST(SUM(amount) AS SIGNED INTEGER) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
			"WHERE idl2.idsrc = 'steam' AND idl2.auth = '" + steamid + "' UNION (SELECT 'steam', '" + steamid + "') ) AS idl WHERE `code` = '" + code + "';"
	).read_all();

	if (!res.empty()){
		return visit(IntegerVisitor<int32_t>(), res[0].values()[0].to_variant());
	}
	return 0;
}

void CHyDatabase::async_GetItemAmountBySteamID(const std::string& steamid, const std::string& code, std::function<void(int32_t)> fn)
{
	auto conn = pimpl->pool.acquire();
	auto sql = std::make_shared<std::string>(
		"SELECT CAST(SUM(amount) AS SIGNED INTEGER) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
		"WHERE idl2.idsrc = 'steam' AND idl2.auth = '" + steamid + "' UNION (SELECT 'steam', '" + steamid + "') ) AS idl WHERE `code` = '" + code + "';"
		);
	conn->async_query(*sql, [fn, conn, sql](boost::system::error_code ec, boost::mysql::tcp_resultset&& resultset) {
		if (ec || !resultset.valid())
			return fn(0);
		auto resultset_keep = std::make_shared<boost::mysql::tcp_resultset>(std::move(resultset));
		resultset_keep->async_read_all([fn, conn, resultset_keep](boost::system::error_code ec, std::vector<boost::mysql::row> res) {
			if (ec)
				return fn(0);
			return fn(!res.empty() ? visit(IntegerVisitor<int32_t>(), res[0].values()[0].to_variant()) : 0);
		});
	});
}

bool CHyDatabase::GiveItemByQQID(int64_t qqid, const std::string & code, int add_amount)
{
    auto conn = pimpl->pool.acquire();
    conn->query("INSERT IGNORE INTO itemown(idsrc, auth, code, amount) VALUES('qq', '" + std::to_string(qqid) + "', '" + code + "', '0');");
	return conn->query("UPDATE itemown SET `amount`=`amount`+'" + std::to_string(add_amount) + "' WHERE `idsrc` = 'qq' AND `auth` ='" + std::to_string(qqid) + "' AND `code` = '" + code + "'").affected_rows() > 0;
}

void CHyDatabase::async_GiveItemByQQID(int64_t qqid, const std::string &code, int add_amount, std::function<void(bool success)> fn)
{
	auto conn = pimpl->pool.acquire();
	auto sql1 = std::make_shared<std::string>("INSERT IGNORE INTO itemown(idsrc, auth, code, amount) VALUES('qq', '" + std::to_string(qqid) + "', '" + code + "', '0'); ");
	auto sql2 = std::make_shared<std::string>("UPDATE itemown SET `amount`=`amount`+'" + std::to_string(add_amount) + "' WHERE `idsrc` = 'qq' AND `auth` ='" + std::to_string(qqid) + "' AND `code` = '" + code + "'");

	conn->async_query(*sql1, [fn, conn, sql1, sql2](boost::system::error_code ec, boost::mysql::tcp_resultset &&resultset){
		if (ec || !resultset.valid())
			return fn(false);
		conn->async_query(*sql2, [fn, conn, sql2](boost::system::error_code ec, boost::mysql::tcp_resultset &&resultset){
			fn(!ec && resultset.affected_rows() > 0);
		});
	});
}

bool CHyDatabase::GiveItemBySteamID(const std::string &steamid, const std::string & code, int add_amount)
{
    auto conn = pimpl->pool.acquire();
    conn->query("INSERT IGNORE INTO itemown(idsrc, auth, code, amount) VALUES('steam', '" + steamid + "', '" + code + "', '0'); ");
	return conn->query("UPDATE itemown SET `amount`=`amount`+'" + std::to_string(add_amount) + "' WHERE `idsrc` = 'steam' AND `auth` ='" + steamid + "' AND `code` = '" + code + "'").affected_rows() > 0;
}

void CHyDatabase::async_GiveItemBySteamID(const std::string &steamid, const std::string &code, int add_amount, std::function<void(bool success)> fn)
{
	auto conn = pimpl->pool.acquire();
	auto sql1 = std::make_shared<std::string>("INSERT IGNORE INTO itemown(idsrc, auth, code, amount) VALUES('steam', '" + steamid + "', '" + code + "', '0'); ");
	auto sql2 = std::make_shared<std::string>("UPDATE itemown SET `amount`=`amount`+'" + std::to_string(add_amount) + "' WHERE `idsrc` = 'steam' AND `auth` ='" + steamid + "' AND `code` = '" + code + "'");

	conn->async_query(*sql1, [fn, conn, sql1, sql2](boost::system::error_code ec, boost::mysql::tcp_resultset &&resultset){
		if (ec || !resultset.valid())
			return fn(false);
		conn->async_query(*sql2, [fn, conn, sql2](boost::system::error_code ec, boost::mysql::tcp_resultset &&resultset){
			fn(!ec && resultset.affected_rows() > 0);
		});
	});
}

bool CHyDatabase::ConsumeItemBySteamID(const std::string &steamid, const std::string & code, int sub_amount)
{
    auto conn = pimpl->pool.acquire();
	if(conn->query("UPDATE itemown SET `amount` = `amount` - '" + std::to_string(sub_amount) + "' WHERE `idsrc` = 'steam' AND `auth` = '" + steamid + "' AND `code` = '" + code + "' AND `amount` > '" + std::to_string(sub_amount) + "'; ").affected_rows() == 1)
		return true;

	int iHasAmount = GetItemAmountBySteamID(steamid, code);
	if(iHasAmount < sub_amount)
		return false;
	iHasAmount -= sub_amount;
    conn->query("DELETE FROM itemown WHERE (itemown.idsrc, itemown.auth) IN (SELECT idl0.idsrc AS idsrc, idl0.auth AS auth FROM idlink AS idl0 JOIN idlink AS idl1 ON idl0.uid = idl1.uid WHERE idl1.idsrc = 'steam' AND idl1.auth = '" + steamid + "') AND `code` = '" + code + "';");
	return GiveItemBySteamID(steamid, code, static_cast<unsigned>(iHasAmount));
}

void CHyDatabase::async_ConsumeItemBySteamID(const std::string& steamid, const std::string& code, int sub_amount, std::function<void(bool success)> fn)
{
	auto ioc = pimpl->ioc;
	auto conn = pimpl->pool.acquire();
	auto sql1 = std::make_shared<std::string>("UPDATE itemown SET `amount` = `amount` - '" + std::to_string(sub_amount) + "' WHERE `idsrc` = 'steam' AND `auth` = '" + steamid + "' AND `code` = '" + code + "' AND `amount` > '" + std::to_string(sub_amount) + "'; ");
	conn->async_query(*sql1, [fn, conn, steamid, code, sql1, sub_amount](boost::system::error_code ec, boost::mysql::tcp_resultset&& resultset) {
		if (ec || !resultset.valid())
			return fn(false);
		
		if (resultset.affected_rows() > 0)
			return fn(true);

		HyDatabase().async_GetItemAmountBySteamID(steamid, code, [fn, conn, steamid, code, sub_amount](int32_t iHasAmount) {
			iHasAmount -= sub_amount;
			auto sql2 = std::make_shared<std::string>("DELETE FROM itemown WHERE (itemown.idsrc, itemown.auth) IN (SELECT idl0.idsrc AS idsrc, idl0.auth AS auth FROM idlink AS idl0 JOIN idlink AS idl1 ON idl0.uid = idl1.uid WHERE idl1.idsrc = 'steam' AND idl1.auth = '" + steamid + "') AND `code` = '" + code + "';");
			conn->async_query(*sql2, [fn, conn, steamid, code, sql2, iHasAmount](boost::system::error_code ec, boost::mysql::tcp_resultset&& resultset) {
				HyDatabase().async_GiveItemBySteamID(steamid, code, iHasAmount, fn);
			});
		});
	});
}

boost::asio::awaitable<std::pair<HyUserSignResultType, std::optional<HyUserSignResult>>> CHyDatabase::async_DoUserDailySign(const HyUserAccountData &user)
{
	if(!user.qqid)
		throw InvalidUserAccountDataException();

	auto ioc = pimpl->ioc;
    auto conn = pimpl->pool.acquire();
    auto pro = std::make_shared<std::promise<std::pair<HyUserSignResultType, std::optional<HyUserSignResult>>>>();

    int rewardmultiply = 1;
    int signcount = 0;

    // 判断是否重复签到
    {
        auto res = co_await (co_await conn->async_query("SELECT TO_DAYS(NOW()) - TO_DAYS(`signdate`) AS signdelta, `signcount` FROM qqevent WHERE `qqid` ='" + std::to_string(user.qqid) + "';", boost::asio::use_awaitable)).async_read_all(boost::asio::use_awaitable);
        if (!res.empty())
        {
            int signdelta = visit(IntegerVisitor<int>(), res[0].values()[0].to_variant());

            if (!res[0].values()[0].is_null() && signdelta == 0 )
            {
                // 已经签到过
                co_return std::pair<HyUserSignResultType, std::optional<HyUserSignResult>>{ HyUserSignResultType::failure_already_signed , std::nullopt };
            }
            if (signdelta == 1)
                signcount = visit(IntegerVisitor<int>(), res[0].values()[1].to_variant());
            co_await conn->async_query("UPDATE qqevent SET `signdate`=NOW(), `signcount`='" + std::to_string(signcount + 1) + "' WHERE `qqid`='" + std::to_string(user.qqid) + "';", boost::asio::use_awaitable);
        }
        else
        {
            co_await conn->async_query("INSERT INTO qqevent(qqid) VALUES('" + std::to_string(user.qqid) + "');", boost::asio::use_awaitable);
        }
    }

    // 计算签到名次
    boost::mysql::tcp_resultset res = co_await conn->async_query("SELECT COUNT(*) FROM qqevent WHERE TO_DAYS(`signdate`) = TO_DAYS(NOW());", boost::asio::use_awaitable);
    int rank = visit(IntegerVisitor(), (co_await res.async_read_all(boost::asio::use_awaitable))[0].values()[0].to_variant());

    if (rank == 1)
        rewardmultiply *= 3;
    else if (rank == 2)
        rewardmultiply *= 5;
    else if (rank == 3)
        rewardmultiply *= 0;
    else if (rank == 4)
        rewardmultiply *= 7;
    else if (rank == 9)
        rewardmultiply *= 0;
    else if (rank == 10)
        rewardmultiply *= 2;

    if (user.access.find('o') != std::string::npos)
        rewardmultiply *= 3;

    ++signcount;

    // 填充签到奖励表
    std::vector<std::pair<HyItemInfo, int32_t>> awards;
    auto res2 = co_await (co_await conn->async_query("SELECT `code`, `name`, `desc`, `quantifier`, `amount` FROM itemaward NATURAL JOIN iteminfo WHERE '" + std::to_string(signcount) + "' BETWEEN `minfrags` AND `maxfrags`;", boost::asio::use_awaitable)).async_read_all(boost::asio::use_awaitable);

    for(auto & l : res2)
    {
        HyItemInfo item = HyItemInfoFromSqlLine(l.values());

        int add_amount = visit(IntegerVisitor(), l.values()[4].to_variant());

        awards.emplace_back(std::move(item), add_amount);
    }

    auto f = [&awards, &user]() -> HyUserSignGetItemInfo {
        // 随机选择签到奖励
        std::random_device rd;
        std::uniform_int_distribution<std::size_t> rg(0, awards.size() - 1);

        auto &reward = awards[rg(rd)];
        auto &item = reward.first;
        auto add_amount = reward.second;

        // 查询已有数量
        auto cur_amount = HyDatabase().GetItemAmountByQQID(user.qqid, item.code);
        cur_amount += add_amount;
        return HyUserSignGetItemInfo{ item, add_amount, cur_amount };
    };
    std::vector<HyUserSignGetItemInfo> vecItems(rewardmultiply);
    std::generate(vecItems.begin(), vecItems.end(), f);

    // 设置新奖励
    for(auto &info : vecItems)
    {
        co_await conn->async_query("INSERT IGNORE INTO itemown(idsrc, auth, code, amount) VALUES('qq', '" + std::to_string(user.qqid) + "', '" + info.item.code + "', '0');", boost::asio::use_awaitable);
        bool result = (co_await conn->async_query("UPDATE itemown SET `amount`=`amount`+'" + std::to_string(info.add_amount) + "' WHERE `idsrc` = 'qq' AND `auth` ='" + std::to_string(user.qqid) + "' AND `code` = '" + info.item.code + "'", boost::asio::use_awaitable)).affected_rows() > 0;
    }
    co_return std::pair<HyUserSignResultType, std::optional<HyUserSignResult>>{ HyUserSignResultType::success, HyUserSignResult{ rank, signcount, rewardmultiply, std::move(vecItems)} };
}

boost::asio::awaitable<std::vector<HyShopEntry>> CHyDatabase::async_QueryShopEntry()
{
	auto ioc = pimpl->ioc;
	auto conn = pimpl->pool.acquire();

    auto code_to_item = [&conn](const std::string &code) -> boost::asio::awaitable<HyItemInfo> {
        co_return HyItemInfoFromSqlLine(
                (co_await (co_await conn->async_query("SELECT `code`, `name`, `desc`, `quantifier` FROM iteminfo WHERE `code` = '" + code + "';", boost::asio::use_awaitable)).async_read_all(boost::asio::use_awaitable)).front().values()
        );
    };

    std::vector<boost::mysql::row> shopres = co_await (co_await conn->async_query(
        "SELECT `shopid`, `target_code`, `target_amount`, `exchange_code`, `exchange_amount` FROM itemshop;"
        , boost::asio::use_awaitable)).async_read_all(boost::asio::use_awaitable);

    std::vector<HyShopEntry> result;
    for(const boost::mysql::row &l : shopres)
    {
        HyShopEntry item {
            visit(IntegerVisitor(), l.values()[0].to_variant()),
            co_await code_to_item(visit(StringVisitor(), l.values()[1].to_variant())),
            visit(IntegerVisitor(), l.values()[2].to_variant()),
            co_await code_to_item(visit(StringVisitor(), l.values()[3].to_variant())),
            visit(IntegerVisitor(), l.values()[4].to_variant())
        };
        result.push_back(item);
    }

    co_return result;
}

void CHyDatabase::Start()
{
	pimpl->pool.reserve(3);
}

void CHyDatabase::Hibernate()
{
	pimpl->pool.clear();
}