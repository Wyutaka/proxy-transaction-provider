i//
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
        } // namespace detail
    }
}