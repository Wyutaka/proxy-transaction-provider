//
// Created by mint25mt on 2022/08/23.
//
//
//#include <iostream>
//#include <memory>
//#include <random>
//#include <string>
//#include <vector>
//
//#include "kvs.h++"
//#include "supports.hpp"
//#include "transaction_provider.h++"
//
//namespace transaction {
//
//        template <class NextF>
//        TransactionProvider<NextF>::TransactionProvider(NextF f)
//                : _next(std::move(f)) {}
//
//        template <class NextF>
//        TransactionProvider<NextF>::~TransactionProvider() = default;
//
//        template <class NextF>
//        Response TransactionProvider<NextF>::next(const Request &request) { return _next(request); }
//
////        template <class NextF>
////        Response insertIfNotExists(const Request &) {
////            return Response({CoResponse(Status::Error)});
////        }
//
//        template <class NextF>
//        Response TransactionProvider<NextF>::operator()(const Request &req) {
//            switch (req.query().type()) {
//                case Query::Type::Begin:
//                    return begin(req);
//
//                case Query::Type::Commit:
//                    return commit(req);
//
//                case Query::Type::Rollback:
//                    return rollback(req);
//
//                case Query::Type::InsertIfNotExists:
////                    return insertIfNotExists(req);
//
//                default:
//                    return query(req);
//            }
//        }
//} // namespace transaction
