//
// Created by mint25mt on 2022/08/23.
//


#include <map>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "../../transaction_provider.h++"
#include "State.h++"

namespace transaction {
    namespace detail {
            State::State()
                    : _s3(nullptr) {
                sqlite3_open_v2(":memory:", &_s3, SQLITE_OPEN_READWRITE, nullptr);
            }

            State::State(const State &) = delete;

            State::State(State &&rhs) noexcept
                    : _s3(rhs._s3), _queries(std::move(rhs._queries)) {
                rhs._s3 = nullptr;
            }

            State::State &operator=(const State &) = delete;

            State::State &operator=(State &&rhs) noexcept {
                State::reset();
                using std::swap;
                swap(State::_s3, rhs._s3);

                State::_queries = std::move(rhs._queries);

                return *this;
            }

            State::~State() { reset(); }

            void State::reset() noexcept {
                if (_s3)
                    sqlite3_close_v2(_s3);
                _s3 = nullptr;
            }

            bool State::executeOnly(const Query &query) noexcept {
                char *errMsg = nullptr;

                auto res =
                        sqlite3_exec(_s3, query.query().data(), nullptr, nullptr, &errMsg);

                return res == SQLITE_OK;
            }

            void State::addQueryOnly(const Query &query) { _queries.emplace_back(query); }

            bool State::add(const Query &query) {
                addQueryOnly(query);
                return executeOnly(query);
            }

            const std::vector <Query> &State::getAllQueries() const { return _queries; }
    } // namespace detail
}
