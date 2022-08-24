//
// Created by MiyaMoto on 2019-07-04.
//

#ifndef TRANSACTION_TRANSACTION_PROVIDER_HPP
#define TRANSACTION_TRANSACTION_PROVIDER_HPP

#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "./src/connector/kvs/slow.hpp"
#include "supports.hpp"

// TODO NOT USE NOW

namespace transaction {
    template <class NextF> class TransactionProvider {
    private:
        NextF _next;

    public:
        explicit TransactionProvider(NextF f)
                : _next(std::move(f)) {}

        virtual ~TransactionProvider() = default;

    protected:
        Response next(const Request &request) { return _next(request); }

    public:
        virtual Response begin(const Request &) = 0;
        virtual Response commit(const Request &) = 0;
        virtual Response rollback(const Request &) = 0;
        virtual Response insertIfNotExists(const Request &) {
            return Response({CoResponse(Status::Error)});
        }

        virtual Response query(const Request &) = 0;

        Response operator()(const Request &req) {
            switch (req.query().type()) {
                case Query::Type::kBegin:
                    return begin(req);

                case Query::Type::kCommit:
                    return commit(req);

                case Query::Type::kRollback:
                    return rollback(req);

                case Query::Type::kInsertIfNotExists:
                    return insertIfNotExists(req);

                default:
                    return query(req);
            }
        }
    };
} // namespace transaction

#endif // TRANSACTION_TRANSACTION_PROVIDER_HPP
