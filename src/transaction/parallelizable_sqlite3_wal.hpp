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
#include "wal_state.h++"

namespace transaction {
    namespace detail {
    } // namespace detail

    template <class NextF>
    class ParallelizableSqlite3WalTransactionProvider : public TransactionProvider<NextF> {
    private:
        std::shared_ptr<journal::MmapPool> _pool;
        std::unordered_map<Peer, detail::WalState, BasicPeerHash<std::string>> _states;

    public:
        using TransactionProvider<NextF>::TransactionProvider;

        explicit ParallelizableSqlite3WalTransactionProvider(NextF f);

    public:
        Response begin(const Request &req);

        Response commit(const Request &req);

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

            auto select_query = "SELECT " + std::string(keys_sv) + " FROM " + std::string(table) + " WHERE " + std::string(keys_sv) + '=' +
                                std::string(values_sv) + " PER PARTITION LIMIT 1";
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
