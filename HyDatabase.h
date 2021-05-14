#pragma once

#include <memory>
#include <string>
#include <chrono>
#include <optional>
#include <vector>
#include <future>
#include <functional>
#include <stdexcept>
#include <system_error>

#include <boost/asio/awaitable.hpp>

struct HyUserAccountData
{
	int64_t qqid = 0;
	std::string name;
	std::string steamid;
	int32_t xscode = 0;
	std::string access = "";
	std::string tag = "未注册";
};

struct HyUserSteamRegisterInfo
{
    int32_t uid;
    int32_t gocode;
};

struct HyItemInfo
{
	std::string code;
	std::string name;
	std::string desc;
	std::string quantifier;
};

struct HyUserOwnItemInfo
{
	HyItemInfo item;
	int32_t amount;
};

struct HyUserSignGetItemInfo
{
	HyItemInfo item;
	int32_t add_amount;
	int32_t cur_amount;
};

enum class HyUserSignResultType
{
	success,
	failure_already_signed,
	failure_unknown
};

struct HyUserSignResult
{
	int iRank;
	int iContinuouslyKeepDays;
	int iMultiply;
	std::vector<HyUserSignGetItemInfo> vecItems;
};

struct HyShopEntry
{
	int32_t shopid;
	HyItemInfo target_item;
	int target_amount;
	HyItemInfo exchange_item;
	int exchange_amount;
};

class InvalidUserAccountDataException : std::invalid_argument {
public:
	InvalidUserAccountDataException() : std::invalid_argument("InvalidUserAccountDataException : 此账号未注册。") {}

	const char *what() const noexcept override
	{
		return "此账号未注册。";
	}
};

class CHyDatabase
{
private:
	CHyDatabase();
	~CHyDatabase();
	friend CHyDatabase &HyDatabase();
    static CHyDatabase instance;

public:
	// 登录用
	HyUserAccountData QueryUserAccountDataByQQID(int64_t qqid) noexcept(false); // 可能抛出InvalidUserAccountDataException
    boost::asio::awaitable<HyUserAccountData> async_QueryUserAccountDataByQQID(int64_t qqid);

	HyUserAccountData QueryUserAccountDataBySteamID(const std::string &steamid) noexcept(false); // 可能抛出InvalidUserAccountDataException
    boost::asio::awaitable<HyUserAccountData> async_QueryUserAccountDataBySteamID(const std::string &steamid);

	// CS1.6支持
	bool UpdateXSCodeByQQID(int64_t qqid, int32_t xscode);
	bool BindQQToCS16Name(int64_t qqid, int32_t xscode);

	// CSGO注册用
	bool BindQQToSteamID(int64_t new_qqid, int32_t gocode);
    boost::asio::awaitable<int32_t> async_StartRegistrationWithSteamID(const std::string& steamid); // 返回gocode

	// 查询服务器里面可用的所有道具类型
	std::vector<HyItemInfo> AllItemInfoAvailable();
	boost::asio::awaitable<std::vector<HyItemInfo>> async_AllItemInfoAvailable();

	// 根据qqid查询名下所有道具（包括绑定的其他账号）
	std::vector<HyUserOwnItemInfo> QueryUserOwnItemInfoByQQID(int64_t qqid);
    boost::asio::awaitable<std::vector<HyUserOwnItemInfo>> async_QueryUserOwnItemInfoByQQID(int64_t qqid);

	// 根据steamid查询名下所有道具（包括绑定的其他账号）
	std::vector<HyUserOwnItemInfo> QueryUserOwnItemInfoBySteamID(const std::string &steamid);
    boost::asio::awaitable<std::vector<HyUserOwnItemInfo>>  async_QueryUserOwnItemInfoBySteamID(const std::string &steamid);

	// 根据qqid查询名下某道具数量
	int32_t GetItemAmountByQQID(int64_t qqid, const std::string & code) noexcept(false);
	void async_GetItemAmountByQQID(int64_t qqid, const std::string& code, std::function<void(int32_t)> fn);

	// 根据steamid查询名下某道具数量
	int32_t GetItemAmountBySteamID(const std::string &steamid, const std::string & code) noexcept(false);
	void async_GetItemAmountBySteamID(const std::string& steamid, const std::string& code, std::function<void(int32_t)> fn);

	// 给玩家qqid赠送道具
	bool GiveItemByQQID(int64_t qqid, const std::string & code, int add_amount);
	void async_GiveItemByQQID(int64_t qqid, const std::string & code, int add_amount, std::function<void(bool success)> fn);

	// 给玩家steamid赠送道具
	bool GiveItemBySteamID(const std::string &steamid, const std::string & code, int add_amount);
	void async_GiveItemBySteamID(const std::string &steamid, const std::string & code, int add_amount, std::function<void(bool success)> fn);

	bool ConsumeItemBySteamID(const std::string &steamid, const std::string & code, int sub_amount);
	void async_ConsumeItemBySteamID(const std::string& steamid, const std::string& code, int sub_amount, std::function<void(bool success)> fn);

	// 签到用（确保QQID存在）
	std::pair<HyUserSignResultType, std::optional<HyUserSignResult>> DoUserDailySign(const HyUserAccountData &user);
    boost::asio::awaitable<std::pair<HyUserSignResultType, std::optional<HyUserSignResult>>> async_DoUserDailySign(const HyUserAccountData &user);

	// 道具商店
    boost::asio::awaitable<std::vector<HyShopEntry>> async_QueryShopEntry();

	// 自动连接
	void Start();

	// 断开所有空闲连接
	void Hibernate();

private:
	struct impl_t;
	std::shared_ptr<impl_t> pimpl;
};

CHyDatabase &HyDatabase();