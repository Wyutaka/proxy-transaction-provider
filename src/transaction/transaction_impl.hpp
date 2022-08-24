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
    template <class NextF>
    class TransactionProviderImpl {
    private:
        std::map<Peer, detail::State> _states;
        NextF _next; // Kc


    public:
//        TransactionProviderImpl(NextF f)
//        : _states(),
//        _next(std::move(f)) {}

        TransactionProviderImpl(SlowCassandraConnector kc)
        : _states(),
        _next(std::move(kc)) {}


    Response next(const Request &request) { return _next(request); }

    public:
        Response begin(const Request &req) {
            _states.emplace(req.peer(), std::move(detail::State()));
            return {CoResponse(Status::Ok)};
        }

        Response commit(const Request &req)  {
            const auto &state = _states[req.peer()];
            const auto &queries = state.getAllQueries();

            auto res = this->next(Request(req.peer(), queries));
            _states.erase(req.peer());
            return res;
        }

        Response rollback(const Request &req) {
            _states.erase(req.peer());
            return {CoResponse(Status::Ok)};
        }

        Response query(const Request &req) {
            auto itr = _states.find(req.peer());
            if (itr == std::end(_states)) {
                return this->next(req);
            }

            itr->second.add(req.query());
            return {CoResponse(Status::Ok)};
        }
    };
    }