//
// Created by y-watanabe on 12/1/22.
//

#ifndef MY_PROXY_THREADPOOL_H
#define MY_PROXY_THREADPOOL_H

#include <thread>
#include <cstdint>
#include <atomic>
#include <mutex>
#include <queue>
#include <functional>
#include <memory>
#include <future>
#include <type_traits>

namespace pool {
    class ThreadPoolExecutor {
        using ui32 = std::uint_fast32_t;

    private:
        mutable std::mutex tasks_queue_mutex{}; // tasksへのアクセスを排他的にする
        std::atomic<bool> is_pool_executor_running{true};
        std::queue<std::function<void()>> tasks{};
        const ui32 thread_count_;
        std::unique_ptr<std::thread[]> threads;
        std::condition_variable checkpoint_task_added;

    public:

        ThreadPoolExecutor(const ui32 &thread_count = std::thread::hardware_concurrency())
                : thread_count_{thread_count ? thread_count : std::thread::hardware_concurrency()} {
            threads.reset(new std::thread[thread_count_]);

            for (ui32 i = 0; i < thread_count_; ++i) {
                threads[i] = std::thread(&ThreadPoolExecutor::worker, this);
            }
        }

        ~ThreadPoolExecutor() {
            {
                std::lock_guard<std::mutex> lock(tasks_queue_mutex);
                is_pool_executor_running = false;
            }
            checkpoint_task_added.notify_all();
            for (ui32 i = 0; i < thread_count_; ++i) {
                threads[i].join();
            }
        }

        ui32 thread_count() const {
            return thread_count_;
        }

        template<typename F, typename... Args, typename R = typename std::result_of<std::decay_t<F>(
                std::decay_t<Args>...)>::type>
        std::future<R> submit(F &&func, const Args &&... args) {
            auto task = std::make_shared<std::packaged_task<R()>>([func, args...]() {
                return func(args...);
            });
            auto future = task->get_future();

            push_task([task]() { (*task)(); });
            return future;
        }

    private:

        template<typename F>
        void push_task(const F &task) {
            {
                const std::lock_guard<std::mutex> lock(tasks_queue_mutex);

                if (!is_pool_executor_running) {
                    throw std::runtime_error("Cannot schedule new task after shutdown.");
                }

                tasks.push(std::function<void()>(task));
            }

            checkpoint_task_added.notify_one();
        }


        void worker() {
            for (;;) {
                std::function<void()> do_task;

                {
                    std::unique_lock<std::mutex> lock(tasks_queue_mutex);
                    checkpoint_task_added.wait(lock, [&] { return !tasks.empty() || !is_pool_executor_running; });

                    if (!is_pool_executor_running && tasks.empty()) {
                        return;
                    }

                    do_task = std::move(tasks.front());
                    tasks.pop();
                }

                do_task();
            }
        }

    };
}

#endif //MY_PROXY_THREADPOOL_H
