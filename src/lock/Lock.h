//
// Created by y-watanabe on 8/22/22.
//

#ifndef MY_PROXY_LOCK_H
#define MY_PROXY_LOCK_H

#include <atomic>
#include "detail.h++"

namespace transaction {
    namespace lock {
        template <class NextF> class Lock {
        private:
            using Ul = std::unique_lock<detail::shared_mutex>;
            using Sl = detail::shared_lock<detail::shared_mutex>;

        public:
            explicit Lock(NextF next)
                    : _next(std::move(next))
                    , _mutex(std::make_unique<detail::shared_mutex>()) {}

        private:
            bool _getLock(const Peer &peer) {
                if (_lock.test_and_set() == true) {
                    return false;
                }

                Ul ul(*_mutex);

                _currentLocker = peer;

                return true;
            }

        public:
            Response operator()(const Request &req) {
                using pipes::operator|;

                // const auto query = req.query() | tolower;

                if (req.query().isBegin()) {
                    if (!_getLock(req.peer())) {
                        return Response({CoResponse(Status::Error)});
                    }
                } else if (req.query().isInsertIfNotExists()) {
                    if (!_getLock(req.peer())) {
                        return Response({CoResponse(Status::Error)});
                    }
                    auto res = _next(req);

                    _lock.clear();
                    return res;

                } else if (req.query().isCommit() || req.query().isRollback()) {
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
        NextF _next;

        detail::MyAtomicFlag _lock;
        std::unique_ptr<detail::shared_mutex> _mutex;
        Peer _currentLocker;
    }
}
#endif //MY_PROXY_LOCK_H
