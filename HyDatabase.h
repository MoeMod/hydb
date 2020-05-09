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
};

struct HyUserSignResult
{
	int iRank;
	int iContinuouslyKeepDays;
	int iMultiply;
	std::vector<HyUserSignGetItemInfo> vecItems;
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
	HyUserAccountData QueryUserAccountDataBySteamID(const std::string &steamid) noexcept(false); // 可能抛出InvalidUserAccountDataException

	// CS1.6支持
	bool UpdateXSCodeByQQID(int64_t qqid, int32_t xscode);
	bool BindQQToCS16Name(int64_t qqid, int32_t xscode);

	// CSGO注册用
	bool BindQQToSteamID(int64_t new_qqid, int32_t gocode);
	int32_t StartRegistrationWithSteamID(const std::string& steamid) noexcept(false);

	// 查询服务器里面可用的所有道具类型
	std::vector<HyItemInfo> AllItemInfoAvailable();
	void async_AllItemInfoAvailable(std::function<void(std::error_code ec, std::vector<HyItemInfo>)> fn);

	// 根据qqid查询名下所有道具（包括绑定的其他账号）
	std::vector<HyUserOwnItemInfo> QueryUserOwnItemInfoByQQID(int64_t qqid);
	void async_QueryUserOwnItemInfoByQQID(int64_t qqid, std::function<void(std::error_code ec, std::vector<HyUserOwnItemInfo>)> fn);

	// 根据steamid查询名下所有道具（包括绑定的其他账号）
	std::vector<HyUserOwnItemInfo> QueryUserOwnItemInfoBySteamID(const std::string &steamid);
	void async_QueryUserOwnItemInfoBySteamID(const std::string &steamid, std::function<void(std::error_code ec, std::vector<HyUserOwnItemInfo>)> fn);

	int32_t GetItemAmountByQQID(int64_t qqid, const std::string & code) noexcept(false);
	int32_t GetItemAmountBySteamID(const std::string &steamid, const std::string & code) noexcept(false);

	// 给玩家qqid赠送道具
	bool GiveItemByQQID(int64_t qqid, const std::string & code, unsigned add_amount);
	void async_GiveItemByQQID(int64_t qqid, const std::string & code, unsigned add_amount, std::function<void(bool success)> fn);
	std::future<bool> async_GiveItemByQQID(int64_t qqid, const std::string & code, unsigned add_amount);

	// 给玩家steamid赠送道具
	bool GiveItemBySteamID(const std::string &steamid, const std::string & code, unsigned add_amount);
	void async_GiveItemBySteamID(const std::string &steamid, const std::string & code, unsigned add_amount, std::function<void(bool success)> fn);
	std::future<bool> async_GiveItemBySteamID(const std::string &steamid, const std::string & code, unsigned add_amount);

	bool ConsumeItemBySteamID(const std::string &steamid, const std::string & code, unsigned sub_amount) noexcept(false);

	// 签到用（确保QQID存在）
	std::pair<HyUserSignResultType, std::optional<HyUserSignResult>> DoUserDailySign(const HyUserAccountData &user);

	// 断开所有空闲连接
	void Hibernate();

private:
	struct impl_t;
	std::shared_ptr<impl_t> pimpl;
};

CHyDatabase &HyDatabase();