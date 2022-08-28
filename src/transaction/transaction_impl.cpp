//
// Created by mint25mt on 2022/08/23.
//

//
// Created by cerussite on 2019/12/21.
//

#include <map>
#include <vector>
#include "../../transaction_provider.h++"
#include "transaction_impl.hpp"

namespace transaction
{
//
//        template<class NextF>
//        Response TransactionProviderImpl<NextF>::begin(const Request &req) {
//            _states.emplace(req.peer(), std::move(detail::State()));
//            return {CoResponse(Status::Ok)};
//        }
//
//        template<class NextF>
//        Response TransactionProviderImpl<NextF>::commit(const Request &req) {
//            const auto &state = _states[req.peer()];
//            const auto &queries = state.getAllQueries();
//
//            auto res = this->next(Request(req.peer(), queries));
//            _states.erase(req.peer());
//            return res;
//        }
//
//        template<class NextF>
//        Response TransactionProviderImpl<NextF>::rollback(const Request &req) {
//            _states.erase(req.peer());
//            return {CoResponse(Status::Ok)};
//        }
//
//        template<class NextF>
//        Response TransactionProviderImpl<NextF>::query(const Request &req) {
//            auto itr = _states.find(req.peer());
//            if (itr == std::end(_states)) {
//                return this->next(req);
//            }
//
//            itr->second.add(req.query());
//            return {CoResponse(Status::Ok)};
//        }
    } // namespace transaction
