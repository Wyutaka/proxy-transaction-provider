//
// Created by user1 on 22/08/27.
//

#ifndef MY_PROXY_SHARED_MUTEX_H
#define MY_PROXY_SHARED_MUTEX_H

#include <atomic>
#include <shared_mutex>
#include <atomic>


namespace transaction {
    namespace lock {
        namespace detail {

            class shared_mutex {
            private:
                std::atomic<int> _shared;
                std::atomic_flag _unique;

            public:
                shared_mutex()
                        : _shared(0)
                        , _unique() {}
                shared_mutex(const shared_mutex &) = delete;
                shared_mutex(shared_mutex &&) = delete;
                shared_mutex &operator=(const shared_mutex &) = delete;
                shared_mutex &operator=(shared_mutex &&) = delete;

            public:
                void lock_shared() {
                    lock();
                    _shared += 1;
                    unlock();
                }

                void unlock_shared() { _shared -= 1; }

            public:
                void lock() {
                    while (_unique.test_and_set())
                        ;
                    while (_shared > 0)
                        ;
                }
                void unlock() { _unique.clear(); }
            };
        }
    }
}

#endif //MY_PROXY_SHARED_MUTEX_H
