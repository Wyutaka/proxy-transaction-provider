//
// Created by MiyaMoto on 2019-07-22.
//

#ifndef TRANSACTION_THROUGH_HPP
#define TRANSACTION_THROUGH_HPP

#include "../../transaction_provider.h++"

namespace transaction {
    template <class NextF> class ThroughTransactionProvider : public TransactionProvider<NextF> {
    public:
        using TransactionProvider<NextF>::TransactionProvider;

    public:
        Response begin(const Request &req) override { return Response({CoResponse(Status::Ok)}); }
        Response commit(const Request &req) override { return Response({CoResponse(Status::Ok)}); }
        Response rollback(const Request &req) override {
            return Response({CoResponse(Status::Ok)});
        }

        Response query(const Request &req) override { return this->next(req); }
    };
} // namespace transaction

#endif // TRANSACTION_THROUGH_HPP
