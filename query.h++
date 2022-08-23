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
            kBegin,
            kCommit,
            kRollback,
            kInsert,
            kInsertIfNotExists,
            kSelect,
            kUpdate,
            kCreate,
            kUnknown,
        };

    private:
        static constexpr Type GetType(std::string_view query) {
            switch (query[0]) {
                case 'b':
                case 'B':
                    return Type::kBegin;

                case 'c':
                case 'C':
                    return (query[1] == 'o' || query[1] == 'O') ? Type::kCommit : Type::kCreate;

                case 'r':
                case 'R':
                    return Type::kRollback;

                case 'i':
                case 'I':
                    return (query.back() == ')') ? Type::kInsert : Type::kInsertIfNotExists;

                case 's':
                case 'S':
                    return Type::kSelect;

                case 'u':
                case 'U':
                    return Type::kUpdate;

                default:
                    return Type::kUnknown;
            }
        }

    private:
        std::string_view _query;
        Type _type;

    public:
        Query() = default;
        explicit Query(const std::string_view &sv)
                : _query(sv)
                , _type(GetType(sv)) {}

        Query(const Query &) = default;
        Query(Query &&) = default;

        Query &operator=(const Query &) = default;
        Query &operator=(Query &&) = default;
        ~Query() = default;

    public:
        [[nodiscard]] const std::string_view &query() const noexcept { return _query; }

    public:
        Type type() const noexcept { return _type; }

#define T(t)                                                                                       \
    bool is##t() const noexcept { return type() == Type::k##t; }
        T(Begin);
        T(Commit);
        T(Rollback);
        T(Insert);
        T(Select);
        T(Update);
        T(Create);
        T(InsertIfNotExists);
#undef T
    };

    inline std::ostream &operator<<(std::ostream &os, const Query &q) {
        os << q.query();
        return os;
    }
} // namespace transaction


#endif //MY_PROXY_QUERY_H
