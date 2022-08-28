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
#include "../../query.h++"

namespace transaction {
    template <class NextF>
    class TransactionProviderImpl {
    private:
        std::map<Peer, detail::State> _states; // std::map<key,value>, peerとstateTable
        NextF _next; // Kc

    public:
//        TransactionProviderImpl(NextF f)
//        : _states(),
//        _next(std::move(f)) {}

        TransactionProviderImpl(SlowCassandraConnector kc)
        : _states(),
        _next(std::move(kc)) {}

    // Response -> std::vector<CoResponse>
    Response next(const Request &request) { return _next(request); } // from_upstream_to downstream

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
            if (itr == std::end(_states)) { // mapの最後
                return this->next(req);
            }

            itr->second.add(req.query());
            return {CoResponse(Status::Ok)};
        }

        Response _normal(Request req) {
            auto itr = _states.find(req.peer());
            if (itr == std::end(_states)) {
                return _next(req); //非トランザクション
            }

            auto rq = static_cast<const Request&&>(req).query();
            itr->second.add((rq)); // イテレータの2番目の値(state)にリクエストを送る
             itr->second.add(req.query());
            return {CoResponse(Status::Ok)};
        }

        Response operator()(Request req) {
            const auto &query = req.query();

            switch (query.type()) {
                case Query::Type::Begin:
                    begin(req);
                    break;
                case Query::Type::Commit:
                    commit(req);
                    break;
                case Query::Type::Rollback:
                    rollback(req);
                    break;
                    // TODO Lwtの実装
//                case Query::Type::InsertIfNotExists: // now not implemented
//                    (std::move(req), res);
//                    break;
                case Query::Type::Unknown:
                    _next(std::move(req));
                    break;
                    // TODO SELECTの実装
//                case Query::Type::Select:
//                    select(std::move(req), res);
//                    break;
                default:
                    _normal(std::move(req));
            }
        }
    };
}