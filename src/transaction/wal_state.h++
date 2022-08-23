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

        public:
            WalState(std::shared_ptr<journal::MmapPool> pool);
            WalState(const WalState &) = delete;
            WalState(WalState &&rhs) noexcept;
            WalState &operator=(const WalState &) = delete;
            WalState &operator=(WalState &&rhs) noexcept;
            ~WalState();

            bool executeOnly(const Query &query) noexcept;
            void addQueryOnly(const Query &query);
            bool add(const Query &query);
            std::vector<Query> getAllQueries() const;

        private:
            void reset() noexcept;

            sqlite3 *_s3;
            journal::MmapJournal _queries;
                    };
    } // namespace detail
} // namespace transaction

#endif //MY_PROXY_WAL_STATE_H
