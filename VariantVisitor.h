#pragma once

#include <variant>
#include <string>
#include <type_traits>

template<class IntegeralType = int>
struct IntegerVisitor {
    static_assert(std::is_integral<IntegeralType>::value);

    template<class T> auto operator()(T arg) -> typename std::enable_if<std::is_integral<T>::value, IntegeralType>::type
    {
        return arg;
    }
    IntegeralType operator()(std::string_view arg)
    {
        std::stringstream ss;
        ss << arg;
        IntegeralType ret = {};
        ss >> ret;
        return ret;
    }
    IntegeralType operator()(std::nullptr_t)
    {
        return 0;
    }
    IntegeralType operator()(...)
    {
        throw std::bad_variant_access();
    }
};
IntegerVisitor() -> IntegerVisitor<int>;

struct StringVisitor
{
    std::string operator()(std::string_view arg)
    {
        return std::string(arg);
    }
    template<class T>
    auto operator()(T x) -> typename std::enable_if<std::is_integral<T>::value, std::string>::type
    {
        return std::to_string(x);
    }
    std::string operator()(std::nullptr_t)
    {
        return {};
    }
    std::string operator()(...)
    {
        throw std::bad_variant_access();
    }
};