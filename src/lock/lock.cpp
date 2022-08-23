//
// Created by mint25mt on 2022/08/23.
//
#include <atomic>
#include "detail.h++"
#include "supports.hpp"
#include "src/peer/Peer.hpp"
#include "Lock.h++"

namespace transaction {
    namespace lock {
        using Ul = std::unique_lock<detail::shared_mutex>;
        using Sl = detail::shared_lock<detail::shared_mutex>;

        template<class NextF>
        Lock<NextF>::Lock(NextF next)
                : _next(std::move(next)), _mutex(std::make_unique<detail::shared_mutex>()) {}

        template<class NextF>
        bool Lock<NextF>::_getLock(const Peer &peer) {
            // ip port

            if (_lock.test_and_set() == true) {
                return false;
            }

            Ul ul(*_mutex);

            _currentLocker = peer;

            return true;
        }

        template<class NextF>
        Response Lock<NextF>::operator()(const Request &req) {
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
    }
}