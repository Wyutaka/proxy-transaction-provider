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
        Query();
        explicit Query(const std::string_view &sv);
        Query(const Query &);
        Query(Query &&);
        Query &operator=(const Query &);
        Query &operator=(Query &&);
        ~Query();
        [[nodiscard]] const std::string_view &query() const noexcept;

        Type type() const noexcept;


#define T(t)                                                                                       \
    bool is##t() const noexcept;
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
        static constexpr Type GetType(std::string_view query);

        std::string_view _query;
        Type _type;
    };

    inline std::ostream &operator<<(std::ostream &os, const Query &q);

} // namespace transaction
#endif //MY_PROXY_QUERY_H
