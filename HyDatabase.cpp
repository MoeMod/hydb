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
#include <boost/asio/spawn.hpp>

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
static HyUserAccountData UserAccountDataFromSqlResult(const std::vector<boost::mysql::owning_row> &res)
{
	if (res.empty())
		throw InvalidUserAccountDataException();
	const auto &line = res[0].values();
	return HyUserAccountData{
		std::visit(IntegerVisitor<int64_t>(), line[0]),
		std::visit(StringVisitor(), line[1]),
		std::visit(StringVisitor(), line[2]),
		std::visit(IntegerVisitor<int32_t>(), line[3]),
		std::visit(StringVisitor(), line[4]),
		std::visit(StringVisitor(), line[5])
	};
}

HyUserAccountData CHyDatabase::QueryUserAccountDataByQQID(int64_t fromQQ)
{
	return UserAccountDataFromSqlResult(pimpl->pool.query_fetch(
		"SELECT qqid, name, steamid, xscode, access, tag FROM qqlogin "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS qqid, uid FROM idlink WHERE idsrc = 'qq') AS T1 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS name, uid FROM idlink WHERE idsrc = 'name') AS T2 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS steamid, uid FROM idlink WHERE idsrc = 'steam') AS T3 "
		"WHERE `qqid` = '" + std::to_string(fromQQ) + "';"
	));
}

void CHyDatabase::async_QueryUserAccountDataByQQID(int64_t fromQQ, std::function<void(std::optional<HyUserAccountData>)> fn)
{
	auto sql = std::make_shared<std::string>(
		"SELECT qqid, name, steamid, xscode, access, tag FROM qqlogin "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS qqid, uid FROM idlink WHERE idsrc = 'qq') AS T1 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS name, uid FROM idlink WHERE idsrc = 'name') AS T2 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS steamid, uid FROM idlink WHERE idsrc = 'steam') AS T3 "
		"WHERE `qqid` = '" + std::to_string(fromQQ) + "';"
	);
	auto conn = pimpl->pool.acquire();
	conn->async_query(*sql, [sql, fn, conn](boost::system::error_code ec, boost::mysql::tcp_resultset &&resultset) {
		if (ec || !resultset.valid())
			return fn(std::nullopt);
		auto resultset_keep = std::make_shared<boost::mysql::tcp_resultset>(std::move(resultset));
		resultset_keep->async_fetch_all([fn, conn, resultset_keep](boost::system::error_code ec, std::vector<boost::mysql::owning_row> res){
			if (ec)
				return fn(std::nullopt);
			try {
				return fn(UserAccountDataFromSqlResult(res));
			}
			catch(...) {
				return fn(std::nullopt);
			}
		});
	});
}

HyUserAccountData CHyDatabase::QueryUserAccountDataBySteamID(const std::string& steamid) noexcept(false)
{
	return UserAccountDataFromSqlResult(pimpl->pool.query_fetch(
		"SELECT qqid, name, steamid, xscode, access, tag FROM qqlogin "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS qqid, uid FROM idlink WHERE idsrc = 'qq') AS T1 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS name, uid FROM idlink WHERE idsrc = 'name') AS T2 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS steamid, uid FROM idlink WHERE idsrc = 'steam') AS T3 "
		"WHERE `steamid` = '" + steamid + "';"
	));
}

void CHyDatabase::async_QueryUserAccountDataBySteamID(const std::string &steamid, std::function<void(std::optional<HyUserAccountData>)> fn)
{
	auto sql = std::make_shared<std::string>(
		"SELECT qqid, name, steamid, xscode, access, tag FROM qqlogin "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS qqid, uid FROM idlink WHERE idsrc = 'qq') AS T1 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS name, uid FROM idlink WHERE idsrc = 'name') AS T2 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS steamid, uid FROM idlink WHERE idsrc = 'steam') AS T3 "
		"WHERE `steamid` = '" + steamid + "';"
		);
	auto conn = pimpl->pool.acquire();
	conn->async_query(*sql, [sql, fn, conn](boost::system::error_code ec, boost::mysql::tcp_resultset &&resultset) {
		if (ec || !resultset.valid())
			return fn(std::nullopt);
		auto resultset_keep = std::make_shared<boost::mysql::tcp_resultset>(std::move(resultset));
		resultset_keep->async_fetch_all([fn, conn, resultset_keep](boost::system::error_code ec, std::vector<boost::mysql::owning_row> res){
			if (ec)
				return fn(std::nullopt);
			try {
				return fn(UserAccountDataFromSqlResult(res));
			}
			catch(...) {
				return fn(std::nullopt);
			}
		});
	});
}

bool CHyDatabase::UpdateXSCodeByQQID(int64_t qqid, int32_t xscode)
{
	auto res1 = pimpl->pool.query_update("UPDATE qqlogin SET `xscode` = '" + std::to_string(xscode) + "' WHERE `qqid` = '" + std::to_string(qqid) + "';");
	return res1 == 1;
}

bool CHyDatabase::BindQQToCS16Name(int64_t new_qqid, int32_t xscode)
{
	auto res1 = pimpl->pool.query_fetch("SELECT `name` FROM cs16reg WHERE `xscode` = '" + std::to_string(xscode) + "';");
	if (res1.empty())
		return false;
	
	const std::string name = std::string(std::get<std::string_view>(res1[0].values()[0]));
	int uid = 0;
	while (1)
	{
		auto res2 = pimpl->pool.query_fetch("SELECT `uid` FROM idlink WHERE `idsrc` = 'qq' AND `auth` = '" + std::to_string(new_qqid) + "';");
		if (res2.empty())
		{
			//没有注册过，插入新的uid
            pimpl->pool.query_update("INSERT IGNORE INTO idlink(idsrc, auth) VALUES('qq', '" + std::to_string(new_qqid) + "');");
            pimpl->pool.query_update("INSERT IGNORE INTO qqlogin(qqid) VALUES('" + std::to_string(new_qqid) + "');");
			continue;
		}
		//已经注册过，得到原先的uid 
		uid = std::visit(IntegerVisitor<int>(), res2[0].values()[0]);
		break;
	}
	//用uid和steamid注册
	auto res3 = pimpl->pool.query_update("INSERT IGNORE INTO idlink(idsrc, auth, uid) VALUES('name', '" + name + "', '" + std::to_string(uid) + "');");
	//删掉cs16reg里面的表项，不管成不成功都无所谓了
    pimpl->pool.query_update("DELETE FROM cs16reg WHERE `name` = '" + name + "';");
	return res3 == 1;
}

bool CHyDatabase::BindQQToSteamID(int64_t new_qqid, int32_t gocode)
{
	auto res1 = pimpl->pool.query_fetch("SELECT `steamid` FROM csgoreg WHERE `gocode` = '" + std::to_string(gocode) + "';");
	if (res1.empty())
		return false; // 没有记录的注册id

	const std::string steamid = std::string(std::get<std::string_view>(res1[0].values()[0]));
	int uid = 0; 
	while (1)
	{
		auto res2 = pimpl->pool.query_fetch("SELECT `uid` FROM idlink WHERE `idsrc` = 'qq' AND `auth` = '" + std::to_string(new_qqid) + "';");
		if (res2.empty())
		{
			//没有注册过，插入新的uid
            pimpl->pool.query_update("INSERT IGNORE INTO idlink(idsrc, auth) VALUES('qq', '" + std::to_string(new_qqid) + "');");
            pimpl->pool.query_update("INSERT IGNORE INTO qqlogin(qqid) VALUES('" + std::to_string(new_qqid) + "');");
			continue;
		}
		//已经注册过，得到原先的uid 
		uid = std::visit(IntegerVisitor<int>(), res2[0].values()[0]);
		break;
	}
	//用uid和steamid注册
	auto res3 = pimpl->pool.query_update("INSERT IGNORE INTO idlink(idsrc, auth, uid) VALUES('steam', '" + steamid + "', '" + std::to_string(uid) + "');");
	//删掉csgoreg里面的表项，不管成不成功都无所谓了
    pimpl->pool.query_update("DELETE FROM csgoreg WHERE `steamid` = '" + steamid + "';");
	return res3 == 1;
}

void CHyDatabase::async_StartRegistrationWithSteamID(const std::string& steamid, std::function<void(int32_t gocode)> fn)
{
	auto ioc = pimpl->ioc;
	auto conn = pimpl->pool.acquire();
	boost::asio::spawn(ioc->get_executor(), [ioc, conn, steamid, fn](boost::asio::yield_context yield) {

		static std::random_device rd;
		std::string steamid_hash = std::to_string(std::hash<std::string>()(steamid));
		std::string gocode(8, '0');
		while(steamid_hash.size() < gocode.size())
			steamid_hash.push_back(std::uniform_int_distribution<int>('0', '9')(rd));
		int iMaxTries = 10;
		do {
			if (--iMaxTries == 0)
				ioc->dispatch(std::bind(fn, 0));
			conn->async_query("DELETE FROM csgoreg WHERE `steamid` = '" + steamid + "';", yield);
			std::sample(steamid_hash.begin(), steamid_hash.end(), gocode.begin(), gocode.size(), std::mt19937(rd()));
		} while (conn->async_query("INSERT IGNORE INTO csgoreg(steamid, gocode) VALUES('" + steamid + "', '" + gocode+ "');", yield).affected_rows() != 1);
		return ioc->dispatch(std::bind(fn, std::stoi(gocode)));
	});
}

// `code`, `name`, `desc`, `quantifier`
static HyItemInfo HyItemInfoFromSqlLine(const std::vector<boost::mysql::value> &line)
{
	return HyItemInfo{
			std::string(std::get<std::string_view>(line[0])),
			std::string(std::get<std::string_view>(line[1])),
			std::string(std::get<std::string_view>(line[2])),
			std::string(std::get<std::string_view>(line[3]))
	};
}

static std::vector<HyItemInfo> InfoListFromSqlResult(const std::vector<boost::mysql::owning_row> & res)
{
	std::vector<HyItemInfo> result;
	for(auto &l : res)
	{
		result.push_back(HyItemInfoFromSqlLine(l.values()));
	}
	return result;
}

// `code`, `name`, `desc`, `quantifier`, `amount`
static std::vector<HyUserOwnItemInfo> UserOwnItemInfoListFromSqlResult(const std::vector<boost::mysql::owning_row> &res)
{
    std::vector<HyUserOwnItemInfo> result;
	for(auto &l : res)
	{
		result.push_back({ HyItemInfoFromSqlLine(l.values()), std::visit(IntegerVisitor<int>(), l.values()[4]) });
	}
    return result;
}

std::vector<HyItemInfo> CHyDatabase::AllItemInfoAvailable() noexcept(false)
{
	return InfoListFromSqlResult(pimpl->pool.query_fetch(
		"SELECT `code`, `name`, `desc`, `quantifier` FROM iteminfo;"
	));
}

void CHyDatabase::async_AllItemInfoAvailable(std::function<void(std::error_code, std::vector<HyItemInfo>)> fn)
{
	auto conn = pimpl->pool.acquire();
	conn->async_query("SELECT `code`, `name`, `desc`, `quantifier` FROM iteminfo;", [fn, conn](boost::system::error_code ec, boost::mysql::tcp_resultset &&resultset) {
		if (ec || !resultset.valid())
			return fn(ec, {});
		auto resultset_keep = std::make_shared<boost::mysql::tcp_resultset>(std::move(resultset));
		resultset_keep->async_fetch_all([fn, conn, resultset_keep](boost::system::error_code ec, std::vector<boost::mysql::owning_row> res){
			return fn(ec, InfoListFromSqlResult(res));
		});
	});
}

std::vector<HyUserOwnItemInfo> CHyDatabase::QueryUserOwnItemInfoByQQID(int64_t qqid)
{
	return UserOwnItemInfoListFromSqlResult(pimpl->pool.query_fetch(
		"SELECT `code`, `name`, `desc`, `quantifier`, `amount` FROM iteminfo NATURAL JOIN ("
			"SELECT code, CAST(SUM(amount) AS SIGNED INTEGER) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
			"WHERE idl2.idsrc = 'qq' AND idl2.auth = '" + std::to_string(qqid) + "' UNION (SELECT 'qq', '" + std::to_string(qqid) + "') ) AS idl GROUP BY code "
		") AS itemlst;"
	));
}

void CHyDatabase::async_QueryUserOwnItemInfoByQQID(int64_t qqid, std::function<void(std::error_code ec, std::vector<HyUserOwnItemInfo>)> fn)
{
	auto conn = pimpl->pool.acquire();
	auto sql = std::make_shared<std::string>(
			"SELECT `code`, `name`, `desc`, `quantifier`, `amount` FROM iteminfo NATURAL JOIN ("
			"SELECT code, CAST(SUM(amount) AS SIGNED INTEGER) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
			"WHERE idl2.idsrc = 'qq' AND idl2.auth = '" + std::to_string(qqid) + "' UNION (SELECT 'qq', '" + std::to_string(qqid) + "') ) AS idl GROUP BY code "
			") AS itemlst;"
		);
	conn->async_query(*sql, [fn, conn, sql](boost::system::error_code ec, boost::mysql::tcp_resultset &&resultset) {
		if (ec || !resultset.valid())
			return fn(ec, {});
		auto resultset_keep = std::make_shared<boost::mysql::tcp_resultset>(std::move(resultset));
		resultset_keep->async_fetch_all([fn, conn, resultset_keep](boost::system::error_code ec, std::vector<boost::mysql::owning_row> res) {
			return fn(ec, UserOwnItemInfoListFromSqlResult(std::move(res)));
		});
	});
}

std::vector<HyUserOwnItemInfo> CHyDatabase::QueryUserOwnItemInfoBySteamID(const std::string &steamid) noexcept(false)
{
	return UserOwnItemInfoListFromSqlResult(pimpl->pool.query_fetch(
			"SELECT `code`, `name`, `desc`, `quantifier`, `amount` FROM iteminfo NATURAL JOIN ("
			"SELECT code, CAST(SUM(amount) AS SIGNED INTEGER) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
			"WHERE idl2.idsrc = 'steam' AND idl2.auth = '" + steamid + "' UNION (SELECT 'steam', '" + steamid + "') ) AS idl GROUP BY code "
			") AS itemlst;"
	));
}

void CHyDatabase::async_QueryUserOwnItemInfoBySteamID(const std::string &steamid, std::function<void(std::error_code ec, std::vector<HyUserOwnItemInfo>)> fn)
{
	auto conn = pimpl->pool.acquire();
	auto sql = std::make_shared<std::string>(
			"SELECT `code`, `name`, `desc`, `quantifier`, `amount` FROM iteminfo NATURAL JOIN ("
			"SELECT code, CAST(SUM(amount) AS SIGNED INTEGER) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
			"WHERE idl2.idsrc = 'steam' AND idl2.auth = '" + steamid + "' UNION (SELECT 'steam', '" + steamid + "') ) AS idl GROUP BY code "
			") AS itemlst;"
	);
	conn->async_query(*sql, [fn, conn, sql](boost::system::error_code ec, boost::mysql::tcp_resultset &&resultset) {
		if (ec || !resultset.valid())
			return fn(ec, {});
		auto resultset_keep = std::make_shared<boost::mysql::tcp_resultset>(std::move(resultset));
		resultset_keep->async_fetch_all([fn, conn, resultset_keep](boost::system::error_code ec, std::vector<boost::mysql::owning_row> res) {
			return fn(ec, UserOwnItemInfoListFromSqlResult(std::move(res)));
		});
	});
}

int32_t CHyDatabase::GetItemAmountByQQID(int64_t qqid, const std::string &code) noexcept(false)
{
	auto res = pimpl->pool.query_fetch(
			"SELECT CAST(SUM(amount) AS SIGNED INTEGER) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
			"WHERE idl2.idsrc = 'qq' AND idl2.auth = '" + std::to_string(qqid) + "' UNION (SELECT 'qq', '" + std::to_string(qqid) + "') ) AS idl WHERE `code` = '" + code + "';"
	);

	if (!res.empty()){
		return std::visit(IntegerVisitor<int32_t>(), res[0].values()[0]);
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
		resultset_keep->async_fetch_all([fn, conn, resultset_keep](boost::system::error_code ec, std::vector<boost::mysql::owning_row> res) {
			if (ec)
				return fn(0);
			return fn(!res.empty() ? std::visit(IntegerVisitor<int32_t>(), res[0].values()[0]) : 0);
			});
		});
}

int32_t CHyDatabase::GetItemAmountBySteamID(const std::string &steamid, const std::string & code) noexcept(false)
{
	auto res = pimpl->pool.query_fetch(
			"SELECT CAST(SUM(amount) AS SIGNED INTEGER) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
			"WHERE idl2.idsrc = 'steam' AND idl2.auth = '" + steamid + "' UNION (SELECT 'steam', '" + steamid + "') ) AS idl WHERE `code` = '" + code + "';"
	);

	if (!res.empty()){
		return std::visit(IntegerVisitor<int32_t>(), res[0].values()[0]);
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
		resultset_keep->async_fetch_all([fn, conn, resultset_keep](boost::system::error_code ec, std::vector<boost::mysql::owning_row> res) {
			if (ec)
				return fn(0);
			return fn(!res.empty() ? std::visit(IntegerVisitor<int32_t>(), res[0].values()[0]) : 0);
		});
	});
}

bool CHyDatabase::GiveItemByQQID(int64_t qqid, const std::string & code, unsigned add_amount)
{
	pimpl->pool.query_update("INSERT IGNORE INTO itemown(idsrc, auth, code, amount) VALUES('qq', '" + std::to_string(qqid) + "', '" + code + "', '0');");
	return pimpl->pool.query_update("UPDATE itemown SET `amount`=`amount`+'" + std::to_string(add_amount) + "' WHERE `idsrc` = 'qq' AND `auth` ='" + std::to_string(qqid) + "' AND `code` = '" + code + "'") > 0;
}

void CHyDatabase::async_GiveItemByQQID(int64_t qqid, const std::string &code, unsigned add_amount, std::function<void(bool success)> fn)
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

bool CHyDatabase::GiveItemBySteamID(const std::string &steamid, const std::string & code, unsigned add_amount)
{
    pimpl->pool.query_update("INSERT IGNORE INTO itemown(idsrc, auth, code, amount) VALUES('steam', '" + steamid + "', '" + code + "', '0'); ");
	return pimpl->pool.query_update("UPDATE itemown SET `amount`=`amount`+'" + std::to_string(add_amount) + "' WHERE `idsrc` = 'steam' AND `auth` ='" + steamid + "' AND `code` = '" + code + "'") > 0;
}

void CHyDatabase::async_GiveItemBySteamID(const std::string &steamid, const std::string &code, unsigned add_amount, std::function<void(bool success)> fn)
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

bool CHyDatabase::ConsumeItemBySteamID(const std::string &steamid, const std::string & code, unsigned sub_amount)
{
	if(pimpl->pool.query_update("UPDATE itemown SET `amount` = `amount` - '" + std::to_string(sub_amount) + "' WHERE `idsrc` = 'steam' AND `auth` = '" + steamid + "' AND `code` = '" + code + "' AND `amount` > '" + std::to_string(sub_amount) + "'; ") == 1)
		return true;

	int iHasAmount = GetItemAmountBySteamID(steamid, code);
	if(iHasAmount < sub_amount)
		return false;
	iHasAmount -= sub_amount;
    pimpl->pool.query_update("DELETE FROM itemown WHERE (itemown.idsrc, itemown.auth) IN (SELECT idl0.idsrc AS idsrc, idl0.auth AS auth FROM idlink AS idl0 JOIN idlink AS idl1 ON idl0.uid = idl1.uid WHERE idl1.idsrc = 'steam' AND idl1.auth = '" + steamid + "') AND `code` = '" + code + "';");
	return GiveItemBySteamID(steamid, code, static_cast<unsigned>(iHasAmount));
}

void CHyDatabase::async_ConsumeItemBySteamID(const std::string& steamid, const std::string& code, unsigned sub_amount, std::function<void(bool success)> fn)
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

std::pair<HyUserSignResultType, std::optional<HyUserSignResult>> CHyDatabase::DoUserDailySign(const HyUserAccountData &user)
{
	return async_DoUserDailySign(user).get();
}

std::future<std::pair<HyUserSignResultType, std::optional<HyUserSignResult>>> CHyDatabase::async_DoUserDailySign(const HyUserAccountData &user)
{
	if(!user.qqid)
		throw InvalidUserAccountDataException();

	auto ioc = pimpl->ioc;
    auto conn = pimpl->pool.acquire();
    auto pro = std::make_shared<std::promise<std::pair<HyUserSignResultType, std::optional<HyUserSignResult>>>>();
	boost::asio::spawn(ioc->get_executor(), [ioc, conn, user, pro](boost::asio::yield_context yield){
        int rewardmultiply = 1;
        int signcount = 0;

        // 判断是否重复签到
        {
            auto res = conn->async_query("SELECT TO_DAYS(NOW()) - TO_DAYS(`signdate`) AS signdelta, `signcount` FROM qqevent WHERE `qqid` ='" + std::to_string(user.qqid) + "';", yield).async_fetch_all(yield);
            if (!res.empty())
            {
                int signdelta = std::visit(IntegerVisitor<int>(), res[0].values()[0]);

                if (signdelta == 0 )
                {
                    // 已经签到过
                    return pro->set_value({ HyUserSignResultType::failure_already_signed , std::nullopt });
                }
                if (signdelta == 1)
                    signcount = std::get<std::int32_t>(res[0].values()[1]);
                conn->async_query("UPDATE qqevent SET `signdate`=NOW(), `signcount`='" + std::to_string(signcount + 1) + "' WHERE `qqid`='" + std::to_string(user.qqid) + "';", yield);
            }
            else
            {
                conn->async_query("INSERT INTO qqevent(qqid) VALUES('" + std::to_string(user.qqid) + "');", yield);
            }
        }

        // 计算签到名次
        boost::mysql::tcp_resultset res = conn->async_query("SELECT COUNT(*) FROM qqevent WHERE TO_DAYS(`signdate`) = TO_DAYS(NOW());", yield);
        int rank = std::visit(IntegerVisitor(), res.async_fetch_all(yield)[0].values()[0]);

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
        {
            auto res = conn->async_query("SELECT `code`, `name`, `desc`, `quantifier`, `amount` FROM itemaward NATURAL JOIN iteminfo WHERE '" + std::to_string(signcount) + "' BETWEEN `minfrags` AND `maxfrags`;", yield).async_fetch_all(yield);

            for(auto & l : res)
            {
                HyItemInfo item = HyItemInfoFromSqlLine(l.values());

                int add_amount = std::visit(IntegerVisitor(), l.values()[4]);

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
				conn->async_query("INSERT IGNORE INTO itemown(idsrc, auth, code, amount) VALUES('qq', '" + std::to_string(user.qqid) + "', '" + info.item.code + "', '0');", yield);
				bool result = conn->async_query("UPDATE itemown SET `amount`=`amount`+'" + std::to_string(info.add_amount) + "' WHERE `idsrc` = 'qq' AND `auth` ='" + std::to_string(user.qqid) + "' AND `code` = '" + info.item.code + "'", yield).affected_rows() > 0;
			}
            return pro->set_value({ HyUserSignResultType::success, HyUserSignResult{ rank, signcount, rewardmultiply, std::move(vecItems)} });
        }
	});
    return pro->get_future();
}

void CHyDatabase::Start()
{
	pimpl->pool.reserve(3);
}

void CHyDatabase::Hibernate()
{
	pimpl->pool.clear();
}