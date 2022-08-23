//
// Created by MiyaMoto on 2019-07-04.
//

#ifndef TRANSACTION_PARALLELIZABLE_OFFICIAL_SQLITE3_HPP
#define TRANSACTION_PARALLELIZABLE_OFFICIAL_SQLITE3_HPP

#include <map>
#include <regex>
#include <shared_mutex>
#include <sstream>
#include <unordered_map>

#include <boost/algorithm/string/join.hpp>
#include "../journal/mmap_journal.hpp"
#include <sqlite3.h>

#include "../../transaction_provider.h++"

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

    template <class NextF>
    class ParallelizableSqlite3WalTransactionProvider : public TransactionProvider<NextF> {
    private:
        std::shared_ptr<journal::MmapPool> _pool;
        std::unordered_map<Peer, detail::WalState, BasicPeerHash<std::string>> _states;

    public:
        using TransactionProvider<NextF>::TransactionProvider;

        explicit ParallelizableSqlite3WalTransactionProvider(NextF f)
            : TransactionProvider<NextF>(std::move(f))
            , _pool(journal::MmapPool::New("./data", 16_KiB, 10)) {}

    public:
        Response begin(const Request &req) override {
            _states.emplace(req.peer(), std::move(detail::WalState(_pool)));
            return {CoResponse(Status::Ok)};
        }

        Response commit(const Request &req) override {
            const auto &state = _states.at(req.peer());
            const auto &queries = state.getAllQueries();

            auto res = this->next(Request(req.peer(), queries));
            _states.erase(req.peer());
            return res;
        }

        Response rollback(const Request &req) override {
            _states.erase(req.peer());
            return {CoResponse(Status::Ok)};
        }

        Response insertIfNotExists(const Request &req) override {
            static const std::regex INSERT_IF_NOT_EXISTS(
                R"(INSERT\s+INTO\s+([\w\d.]+)\s*\(([\w\d,\s]+)\)\s*VALUES\s*\(([\w\d,\s']+)\)\s+IF\s+NOT\s+EXISTS)",
                std::regex_constants::icase);

            auto query = req.query().query();

            std::cmatch cm;
            if (!std::regex_match(std::begin(query), std::end(query), cm, INSERT_IF_NOT_EXISTS)) {
                return Response({CoResponse(Status::Error)});
            }
            auto table = std::string_view(cm[1].first, cm[1].length());

            auto keys_sv = std::string_view(cm[2].first, cm[2].length());
            keys_sv = keys_sv.substr(0, keys_sv.find(','));

            auto values_sv = std::string_view(cm[3].first, cm[3].length());
            values_sv = values_sv.substr(0, values_sv.find(','));

            auto select_query = "SELECT " + keys_sv.data() + " FROM " + table + " WHERE " + keys_sv.data() + '=' +
                                values_sv.data() + " PER PARTITION LIMIT 1";
            auto select = this->next(Request(req.peer(), select_query));

            if (select.front().data().empty()) {
                query.remove_suffix(13); // IF NOT EXISTS 13

                return this->next({req.peer(), Query(query)});
            }
            return Response({CoResponse(Status::Ok)});
        }

        Response query(const Request &req) override {
            auto itr = _states.find(req.peer());
            if (itr == std::end(_states)) {
                return this->next(req);
            }

            itr->second.add(req.query());
            return {CoResponse(Status::Ok)};
        }
    };
} // namespace transaction

#endif // TRANSACTION_PARALLELIZABLE_OFFICIAL_SQLITE3_HPP
