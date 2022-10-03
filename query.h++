//
// Created by mint25mt on 2022/08/23.
//

#ifndef MY_PROXY_QUERY_H
#define MY_PROXY_QUERY_H

#pragma once

#include <string_view>

namespace transaction {
    class Query {
    public:
        enum class Type {
            Begin,
            Commit,
            Rollback,
            Insert,
            InsertIfNotExists,
            Select,
            Update,
            Create,
            Unknown,
        };

    public:
        Query() = default;
        explicit Query(const std::string_view sv)
                : _query(std::string(sv))
                , _type(GetType(sv)) {
//                        std::cout << "req.query():" << _query << std::endl; // ここはok
        }

        Query(const Query &) = default;
        Query(Query &&) = default;

        Query &operator=(const Query &) = default;
        Query &operator=(Query &&) = default;
        ~Query() = default;

        [[nodiscard]] Type type() const noexcept { return _type; };

//        [[nodiscard]] const std::string_view &query() const noexcept {
            [[nodiscard]] std::string_view query() const noexcept {
//            std::cout << "req.query() aadfa: " << _query << std::endl; // ここはだめ...
            return _query; }

#define T(t)                                                                                       \
    bool is##t() const noexcept { return type() == Type::t; }
        T(Begin)
        T(Commit)
        T(Rollback)
        T(Insert)
        T(Select)
        T(Update)
        T(Create)
        T(InsertIfNotExists)
#undef T

    private:
        static constexpr Type GetType(std::string_view query)
        {
            switch (query[0]) {
                case 'b':
                case 'B':
                    return Query::Type::Begin;

                case 'c':
                case 'C':
                    return (query[1] == 'o' || query[1] == 'O') ? Query::Type::Commit : Query::Type::Create;

                case 'r':
                case 'R':
                    return Query::Type::Rollback;

                case 'i':
                case 'I':
                    return Query::Type::Insert;

                case 's':
                case 'S':
                    return Query::Type::Select;

                case 'u':
                case 'U':
                    return Query::Type::Update;

                default:
                    return Query::Type::Unknown;
            }
        }

    private:
//        std::string_view _query;
        std::string _query;
        Type _type;
    };

    inline std::ostream &operator<<(std::ostream &os, const Query &q){
        os << q.query();
        return os;
    }

} // namespace transaction
#endif //MY_PROXY_QUERY_H
