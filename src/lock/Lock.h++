//
// Created by y-watanabe on 8/22/22.
//

#ifndef MY_PROXY_LOCK_H
#define MY_PROXY_LOCK_H

#include <atomic>
#include "detail.h++"
#include "supports.hpp"
#include "src/peer/Peer.hpp"
#include "src/reqestresponse/Request.h++"
#include "detail.h++"
#include "shared_mutex.h++"

namespace transaction::lock {
        template<class NextF> class Lock {
        private:
            using Ul = std::unique_lock<detail::shared_mutex>;
            using Sl = detail::shared_lock<detail::shared_mutex>;

        public:
            explicit Lock(NextF next)
                    : _next(std::move(next)), _mutex(std::make_unique<detail::shared_mutex>()) {}

//            Response operator()(const Request &req);

            Response operator()(const Request &req) {
                using pipes::operator|;
            // const auto query = req.query() | tolower;


                if (req.query().isBegin()) {
                if (!_getLock(req.peer())) {

                    return Response({CoResponse(Status::Error)});
                }
            } else if (req.query().isInsertIfNotExists() || req.query().isCommit() || req.query().isRollback()) {
                if (!_getLock(req.peer()) && req.query().isInsertIfNotExists()) {
                    return Response({CoResponse(Status::Error)});
                }

                auto res = _next(req);

                _lock.clear();
                return res;
            } else if (_lock.test()) {

                    Sl sl(*_mutex);
                if (req.peer() != _currentLocker) {
                    return Response({CoResponse(Status::Error)});
                }
            }

            return _next(req);
            }


        private:
            bool _getLock(const Peer &peer) {
                // ip port

                if (_lock.test_and_set() == true) {
                    return false;
                }

                Ul ul(*_mutex);

                _currentLocker = peer;

                return true;
            }

            NextF _next; // transaction
            detail::MyAtomicFlag _lock;
            std::unique_ptr<detail::shared_mutex> _mutex;
            Peer _currentLocker;
        };
    }
#endif //MY_PROXY_LOCK_H
