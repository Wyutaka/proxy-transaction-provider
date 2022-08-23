//
// Created by mint25mt on 2022/08/23.
//

#ifndef MY_PROXY_ROWDATA_H
#define MY_PROXY_ROWDATA_H

#include <string>
#include <unordered_map>
#include <vector>
#include <string_view>

#include "query.h++"
#include <boost/algorithm/string/replace.hpp>
#include <any>

namespace transaction {
        class RowData {
        public:
            enum class Type : std::uint16_t {
                Unknown = 0,
                Int = 0x0009,
                String = 0x0001,
            };

        private:
            std::any _data;
            Type _type;

        public:
            template <class IntegralT>
            explicit RowData(
                    IntegralT n,
                    typename std::enable_if<std::is_integral<IntegralT>::value>::type *nn = nullptr)
                    : _data(static_cast<int>(n))
                    , _type(Type::Int) {}

            RowData()
                    : _data()
                    , _type(Type::Unknown) {}

            explicit RowData(std::string s)
                    : _data(std::move(s))
                    , _type(Type::String) {}

            RowData(const RowData &) = default;
            RowData(RowData &&) = default;

            RowData &operator=(const RowData &) = default;
            RowData &operator=(RowData &&) = default;

        public:
            [[nodiscard]] Type type() const noexcept { return _type; }
            template <class T>[[nodiscard]] T get() const { return std::any_cast<T>(_data); }

            [[nodiscard]] std::string str() const {
                switch (type()) {
                    case Type::Int:
                        return std::to_string(get<int>());
                    case Type::String:
                        return get<std::string>();
                    default:
                        return "null";
                }
            }

            [[nodiscard]] std::string queryStr() const {
                switch (type()) {
                    case Type::Int:
                        return std::to_string(get<int>());
                    case Type::String: {
                        auto query = get<std::string>();
                        // boost::algorithm::replace_all(query, "'", "''");
                        return '\'' + query + '\'';
                    }
                    default:
                        return "null";
                }
            }
        };
}

#endif //MY_PROXY_ROWDATA_H
