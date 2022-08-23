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
        class TransactionProviderImpl : public TransactionProvider<NextF> {
        private:

        public:
            std::map<Peer, detail::State> _states;
            using TransactionProvider<NextF>::TransactionProvider;

        public:
            Response begin(const Request &req);

            Response commit(const Request &req);

            Response rollback(const Request &req);

            Response query(const Request &req);
        };
    } // namespace transaction
}