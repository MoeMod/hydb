#pragma once

#include <memory>
#include <string>
#include <chrono>
#include <optional>
#include <vector>
#include <stdexcept>

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

	// CSGO注册用
	bool BindQQToSteamID(int64_t new_qqid, int32_t gocode);
	int32_t StartRegistrationWithSteamID(const std::string& steamid) noexcept(false);

	// 查道具用
	std::vector<HyUserOwnItemInfo> QueryUserOwnItemInfoByQQID(int64_t qqid) noexcept(false);
	std::vector<HyUserOwnItemInfo> QueryUserOwnItemInfoBySteamID(const std::string &steamid) noexcept(false);
	int32_t GetItemAmountByQQID(int64_t qqid, const std::string & code) noexcept(false);
	int32_t GetItemAmountBySteamID(const std::string &steamid, const std::string & code) noexcept(false);
	bool GiveItemByQQID(int64_t qqid, const std::string & code, unsigned add_amount) noexcept(false);
	bool GiveItemBySteamID(const std::string &steamid, const std::string & code, unsigned add_amount) noexcept(false);

	// 签到用（确保QQID存在）
	std::pair<HyUserSignResultType, std::optional<HyUserSignResult>> DoUserDailySign(const HyUserAccountData &user);

	// 断开所有空闲连接
	void Hibernate();

private:
	struct impl_t;
	std::shared_ptr<impl_t> pimpl;
};

CHyDatabase &HyDatabase();