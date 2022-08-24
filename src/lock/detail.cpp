//
// Created by mint25mt on 2022/08/23.
//

#include <atomic>
#include <shared_mutex>
#include "detail.h++"

namespace transaction {
    namespace lock {
        namespace detail {
                bool MyAtomicFlag::test_and_set() noexcept {
                    std::uint8_t test = 1;
                    __asm__ __volatile__("xchgb %%al, %%dl"
                            : "=a"(test), "=d"(_flag)
                            : "a"(test), "d"(_flag));
                    return static_cast<bool>(test);
                }

                bool MyAtomicFlag::test() const noexcept {
                    std::uint8_t test;
                    __asm__ __volatile__("movb %%al, %%dl" : "=d"(test) : "a"(_flag));
                    return static_cast<bool>(test);
                }

                void MyAtomicFlag::clear() noexcept { __asm__ __volatile__("movb $0, %0" : "=g"(_flag)); }

                shared_mutex::shared_mutex()
                        : _shared(0), _unique() {}

                void shared_mutex::lock_shared() {
                    lock();
                    _shared += 1;
                    unlock();
                }

                void shared_mutex::unlock_shared() { _shared -= 1; }

                void shared_mutex::lock() {
                    while (_unique.test_and_set());
                    while (_shared > 0);
                }

                void shared_mutex::unlock() { _unique.clear(); }

                template<class MtxT>
                shared_lock<MtxT>::shared_lock(MtxT &mtx)
                        : _mutex(mtx) {
                    _mutex.lock_shared();
                }

                template<class MtxT>
                shared_lock<MtxT>::~shared_lock() { _mutex.unlock_shared(); }

        } // namespace detail
    }
}