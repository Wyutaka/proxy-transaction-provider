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
            public:
                bool test_and_set() noexcept;
                [[nodiscard]] bool test() const noexcept;
                void clear() noexcept;

            private:
                std::uint8_t _flag = 0;

            };

            class shared_mutex {
            public:
                shared_mutex();

                shared_mutex(const shared_mutex &);

                shared_mutex(shared_mutex &&);

                shared_mutex &operator=(const shared_mutex &);

                shared_mutex &operator=(shared_mutex &&);

                void lock();

                void unlock();

                void lock_shared();

                void unlock_shared();

            private:
                std::atomic<int> _shared;
                std::atomic_flag _unique;
            };

            template<class MtxT>
            class shared_lock {
            public:
                explicit shared_lock(MtxT &mtx);
                ~shared_lock();

            private:
                MtxT &_mutex;
            };

        } // namespace detail
    }
}

#endif //MY_PROXY_DETAIL_H
