//
// Created by y-watanabe on 8/22/22.
//

#ifndef MY_PROXY_DETAIL_H
#define MY_PROXY_DETAIL_H


#include <atomic>
#include <shared_mutex>

namespace transaction {
    namespace lock {
        namespace detail {
            class MyAtomicFlag {
            private:
                std::uint8_t _flag = 0;

            public:
                bool test_and_set() noexcept {
                    std::uint8_t test = 1;
                    __asm__ __volatile__("xchgb %%al, %%dl"
                            : "=a"(test), "=d"(_flag)
                            : "a"(test), "d"(_flag));
                    return static_cast<bool>(test);
                }

                [[nodiscard]] bool test() const noexcept {
                    std::uint8_t test;
                    __asm__ __volatile__("movb %%al, %%dl" : "=d"(test) : "a"(_flag));
                    return static_cast<bool>(test);
                }

                void clear() noexcept { __asm__ __volatile__("movb $0, %0" : "=g"(_flag)); }
            };

            class shared_mutex {
            private:
                std::atomic<int> _shared;
                std::atomic_flag _unique;

            public:
                shared_mutex()
                        : _shared(0), _unique() {}

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
                    while (_unique.test_and_set());
                    while (_shared > 0);
                }

                void unlock() { _unique.clear(); }
            };

            template<class MtxT>
            class shared_lock {
            private:
                MtxT &_mutex;

            public:
                explicit shared_lock(MtxT &mtx)
                        : _mutex(mtx) {
                    _mutex.lock_shared();
                }

                ~shared_lock() { _mutex.unlock_shared(); }
            };

        } // namespace detail
    }
}

#endif //MY_PROXY_DETAIL_H
