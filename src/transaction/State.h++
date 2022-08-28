//
// Created by mint25mt on 2022/08/23.
//

#ifndef MY_PROXY_STATE_H
#define MY_PROXY_STATE_H

#include <map>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "../../transaction_provider.h++"

namespace transaction::detail {
    class State {
    private:
        sqlite3 *_s3;
        std::vector<Query> _queries;

    public:
        State()
                : _s3(nullptr) {
            sqlite3_open_v2(":memory:", &_s3, SQLITE_OPEN_READWRITE, nullptr);
        }

        State(const State &) = delete;
        State(State &&rhs) noexcept
                : _s3(rhs._s3)
                , _queries(std::move(rhs._queries)) {
            rhs._s3 = nullptr;
        }

        State &operator=(const State &) = delete;
        State &operator=(State &&rhs) noexcept {
            reset();
            using std::swap;
            swap(_s3, rhs._s3);

            _queries = std::move(rhs._queries);

            return *this;
        }

        ~State() { reset(); }

    private:
        void reset() noexcept {
            if (_s3)
                sqlite3_close_v2(_s3);
            _s3 = nullptr;
        }

    public:
        bool executeOnly(const Query &query) noexcept {
            char *errMsg = nullptr;

            auto res =
                    sqlite3_exec(_s3, query.query().data(), nullptr, nullptr, &errMsg);

            return res == SQLITE_OK;
        }

        void addQueryOnly(const Query &query) { _queries.emplace_back(query); }

    public:
        bool add(const Query &query) {
            addQueryOnly(query);
            return executeOnly(query);
        }

    public:
        const std::vector<Query> &getAllQueries() const { return _queries; }
    };
    }
#endif //MY_PROXY_STATE_H
