//
// Created by mint25mt on 2022/08/23.
//

#include <string_view>
#include <ostream>
#include "query.h++"

namespace transaction {
        constexpr Query::Type Query::GetType(std::string_view query) {
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
                    return (query.back() == ')') ? Query::Type::Insert : Query::Type::InsertIfNotExists;

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

        Query::Query() = default;
        Query::Query(const std::string_view &sv)
                : _query(sv)
                , _type(GetType(sv)) {}

        Query::Query(const Query &) = default;
        Query::Query(Query &&) = default;

        Query &Query::operator=(const Query &) = default;
        Query &Query::operator=(Query &&) = default;
        Query::~Query() = default;

        [[nodiscard]] const std::string_view &Query::query() const noexcept { return _query; }

        Query::Type Query::type() const noexcept { return _type; }

#define T(t)                                                                                       \
    bool Query::is##t() const noexcept { return type() == Type::t; }
        T(Begin)
        T(Commit)
        T(Rollback)
        T(Insert)
        T(Select)
        T(Update)
        T(Create)
        T(InsertIfNotExists)
#undef T

    inline std::ostream &operator<<(std::ostream &os, const Query &q) {
        os << q.query();
        return os;
    }
} // namespace transaction

