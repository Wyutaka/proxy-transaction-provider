//
// Created by cerussite on 2019/12/21.
//

#pragma once

#include <map>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "../../transaction_provider.h++"
#include "State.h++"

namespace transaction {
    namespace detail {
        template<class NextF>
        class ParallelizableSqlite3TransactionProvider : public TransactionProvider<NextF> {
        private:
            std::map<Peer, detail::State> _states;

        public:
            using TransactionProvider<NextF>::TransactionProvider;

        public:
            Response begin(const Request &req) override {
                _states.emplace(req.peer(), std::move(detail::State()));
                return {CoResponse(Status::Ok)};
            }

            Response commit(const Request &req) override {
                const auto &state = _states[req.peer()];
                const auto &queries = state.getAllQueries();

                auto res = this->next(Request(req.peer(), queries));
                _states.erase(req.peer());
                return res;
            }

            Response rollback(const Request &req) override {
                _states.erase(req.peer());
                return {CoResponse(Status::Ok)};
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
}