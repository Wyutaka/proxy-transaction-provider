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

namespace transaction {
    namespace lock {
        template<class NextF> class Lock {
        private:
            using Ul = std::unique_lock<detail::shared_mutex>;
            using Sl = detail::shared_lock<detail::shared_mutex>;

        public:
            explicit Lock(NextF next)
                    : _next(std::move(next)), _mutex(std::make_unique<detail::shared_mutex>()) {}

            Response operator()(const Request &req);


        private:
            bool _getLock(const Peer &peer) ;

            NextF _next; // transaction
            detail::MyAtomicFlag _lock;
            std::unique_ptr<detail::shared_mutex> _mutex;
            Peer _currentLocker;
        };
    }
}
#endif //MY_PROXY_LOCK_H
