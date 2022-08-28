//
// Created by user1 on 22/08/23.
//

#ifndef MY_PROXY_WAL_STATE_H
#define MY_PROXY_WAL_STATE_H

#include <map>
#include <regex>
#include <shared_mutex>
#include <sstream>
#include <unordered_map>

#include <boost/algorithm/string/join.hpp>
#include "../journal/mmap_journal.hpp"
#include <sqlite3.h>

#include "../../transaction_provider.h++"
#include "transaction_impl.hpp"

namespace transaction {
    namespace detail {
        class WalState {
        public:
            static constexpr bool kAsyncCommit = true;

        private:
            sqlite3 *_s3;
            journal::MmapJournal _queries;

        public:
            WalState(std::shared_ptr<journal::MmapPool> pool)
                    : _s3(nullptr)
                    , _queries(std::move(pool), kAsyncCommit) {
                sqlite3_open_v2(":memory:", &_s3, SQLITE_OPEN_READWRITE, nullptr);
            }

            WalState(const WalState &) = delete;
            WalState(WalState &&rhs) noexcept
                    : _s3(rhs._s3)
                    , _queries(std::move(rhs._queries)) {
                rhs._s3 = nullptr;
            }

            WalState &operator=(const WalState &) = delete;
            WalState &operator=(WalState &&rhs) noexcept {
                reset();
                using std::swap;
                swap(_s3, rhs._s3);

                _queries = std::move(rhs._queries);

                return *this;
            }

            ~WalState() { reset(); }

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

            void addQueryOnly(const Query &query) { _queries.append(query.query()); }

        public:
            bool add(const Query &query) {
                addQueryOnly(query);
                return executeOnly(query);
            }

        public:
            std::vector<Query> getAllQueries() const {
                auto queriesString = _queries.get();
                std::vector<Query> queries(queriesString.size());
                std::transform(std::begin(queriesString), std::end(queriesString),
                               std::begin(queries),
                               [](const std::string_view &query) { return Query(query); });
                return queries;
            }
        };
    } // namespace detail
} // namespace transaction

#endif //MY_PROXY_WAL_STATE_H
