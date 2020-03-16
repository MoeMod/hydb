#include "HyDatabase.h"
#include "MySqlConnectionPool.h"

#include <random>
#include <atomic>

struct CHyDatabase::impl_t
{
public:
	MySqlConnectionPool pool;
	std::atomic<int> m_iCachedSignRank = 0;
};

CHyDatabase CHyDatabase::instance;
CHyDatabase &HyDatabase()
{
	return CHyDatabase::instance;
}

CHyDatabase::CHyDatabase() : pimpl(std::make_shared<impl_t>()) {}

CHyDatabase::~CHyDatabase() = default;

static HyUserAccountData UserAccountDataFromSqlResult(const std::shared_ptr<sql::ResultSet> &res)
{
	if (!res->next())
		throw InvalidUserAccountDataException();
	return HyUserAccountData{
			res->getInt64("qqid"),
			res->getString("name"),
			res->getString("steamid"),
			res->getInt("xscode"),
			res->getString("access"),
			res->getString("tag")
	};
}

HyUserAccountData CHyDatabase::QueryUserAccountDataByQQID(int64_t fromQQ)
{
	return UserAccountDataFromSqlResult(pimpl->pool.acquire()->Query(
		"SELECT qqid, name, steamid, xscode, access, tag FROM qqlogin "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS qqid, uid FROM idlink WHERE idsrc = 'qq') AS T1 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS name, uid FROM idlink WHERE idsrc = 'name') AS T2 "
		"NATURAL LEFT OUTER JOIN (SELECT auth AS steamid, uid FROM idlink WHERE idsrc = 'steam') AS T3 "
		"WHERE `qqid` = '" + std::to_string(fromQQ) + "';"
	));
}

static std::vector<HyUserOwnItemInfo> UserOwnItemInfoListFromSqlResult(const std::shared_ptr<sql::ResultSet> &res)
{
    std::vector<HyUserOwnItemInfo> result;
    while (res->next())
    {
        HyItemInfo item{
                res->getString("code"),
                res->getString("name"),
                res->getString("desc"),
                res->getString("quantifier")
        };
        auto amount = res->getInt("amount");
        result.push_back({ item, amount });
    }
    return result;
}

std::vector<HyUserOwnItemInfo> CHyDatabase::QueryUserOwnItemInfoByQQID(int64_t qqid)
{
	return UserOwnItemInfoListFromSqlResult(pimpl->pool.acquire()->Query(
		"SELECT `code`, `name`, `desc`, `quantifier`, `amount` FROM iteminfo NATURAL JOIN ("
			"SELECT code, SUM(amount) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
			"WHERE idl2.idsrc = 'qq' AND idl2.auth = '" + std::to_string(qqid) + "' UNION (SELECT 'qq', '" + std::to_string(qqid) + "') ) AS idl GROUP BY code "
		") AS itemlst;"
	));
}

int32_t CHyDatabase::GetItemAmountByQQID(int64_t qqid, const std::string &code)
{
	auto conn = pimpl->pool.acquire();
	auto res = conn->Query(
			"SELECT SUM(amount) AS amount FROM itemown NATURAL JOIN (SELECT idl1.idsrc, idl1.auth FROM idlink AS idl1 JOIN idlink AS idl2 ON idl1.uid = idl2.uid "
			"WHERE idl2.idsrc = 'qq' AND idl2.auth = '" + std::to_string(qqid) + "' UNION (SELECT 'qq', '" + std::to_string(qqid) + "') ) AS idl WHERE `code` = '" + code + "';"
	);

	if (res->next()){
		return res->getInt("amount");
	}
	return 0;
}

bool CHyDatabase::GiveItemByQQID(int64_t qqid, const std::string &code, unsigned add_amount) noexcept(false) {
	auto conn = pimpl->pool.acquire();
	pimpl->pool.acquire()->Update("INSERT IGNORE INTO itemown(idsrc, auth, code, amount) VALUES('qq', '" + std::to_string(qqid) + "', '" + code + "', '0'); ");
	return pimpl->pool.acquire()->Update("UPDATE itemown SET `amount`=`amount`+'" + std::to_string(add_amount) + "' WHERE `idsrc` = 'qq' AND `auth` ='" + std::to_string(qqid) + "' AND `code` = '" + code + "'") > 0;
}

std::pair<HyUserSignResultType, std::optional<HyUserSignResult>> CHyDatabase::DoUserDailySign(const HyUserAccountData &user)
{
	if(!user.qqid)
		throw InvalidUserAccountDataException();

	int rewardmultiply = 1;
	int signcount = 0;

	// 判断是否重复签到
	auto conn = pimpl->pool.acquire();
	{
		auto res = conn->Query("SELECT TO_DAYS(NOW()) - TO_DAYS(`signdate`) AS signdelta, `signcount` FROM qqevent WHERE `qqid` ='" + std::to_string(user.qqid) + "';");
		if (res->next())
		{
			auto signdelta = res->getInt(1);
			
			if (!res->isNull(1) && signdelta == 0 )
			{
				// 已经签到过
				return { HyUserSignResultType::failure_already_signed , std::nullopt };
			}

			if (signdelta == 1)
				signcount = res->getInt(2);

			conn->Update("UPDATE qqevent SET `signdate`=NOW(), `signcount`='" + std::to_string(signcount + 1) + "' WHERE `qqid`='" + std::to_string(user.qqid) + "';");
		}
		else
		{
			conn->Update("INSERT INTO qqevent(qqid) VALUES('" + std::to_string(user.qqid) + "');");
		}
	}
		

	// 计算签到名次
	int rank = pimpl->m_iCachedSignRank.load();
	if(rank <= 10)
	{
		auto res = conn->Query("SELECT `qqid` FROM qqevent WHERE TO_DAYS(`signdate`) = TO_DAYS(NOW());");
		rank = res->rowsCount();
		pimpl->m_iCachedSignRank.store(rank);
	}
	else
	{
		++pimpl->m_iCachedSignRank;
	}

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
		auto res = conn->Query("SELECT "
			"amx.itemaward.`code` AS icode, "
			"amx.iteminfo.`name` AS iname, "
			"amx.iteminfo.`desc` AS idesc, "
			"amx.iteminfo.`quantifier` AS iquantifier, "
			"amx.itemaward.`amount` AS iamount "
			"FROM amx.itemaward, amx.iteminfo "
			"WHERE amx.itemaward.`code` = amx.iteminfo.`code` AND '" + std::to_string(signcount) + "' BETWEEN `minfrags` AND `maxfrags`");

		while (res->next())
		{
			HyItemInfo item{
				res->getString("icode"),
				res->getString("iname"),
				res->getString("idesc"),
				res->getString("iquantifier"),
			};
			auto add_amount = res->getInt("iamount");

			awards.emplace_back(std::move(item), add_amount);
		}
	}
	
	auto f = [&awards, &user, &conn, this]() -> HyUserSignGetItemInfo {
		// 随机选择签到奖励
		std::random_device rd;
		std::uniform_int_distribution<std::size_t> rg(0, awards.size() - 1);

		auto &reward = awards[rg(rd)];
		auto &item = reward.first;
		auto add_amount = reward.second;

		// 查询已有数量
		auto cur_amount = GetItemAmountByQQID(user.qqid, item.code);
		cur_amount += add_amount;
		return HyUserSignGetItemInfo{ item, add_amount, cur_amount };
	};
	std::vector<HyUserSignGetItemInfo> vecItems(rewardmultiply);
	std::generate(vecItems.begin(), vecItems.end(), f);

	// 设置新奖励
	auto f2 = [conn = pimpl->pool.acquire(), &user, this](const HyUserSignGetItemInfo &info) {
		GiveItemByQQID(user.qqid, info.item.code, info.add_amount);
	};
	std::for_each(vecItems.begin(), vecItems.end(), f2);

	return { HyUserSignResultType::success, HyUserSignResult{ rank, signcount, rewardmultiply, std::move(vecItems)} };
}