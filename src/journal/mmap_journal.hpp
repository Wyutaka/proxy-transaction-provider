//
// Created by cerussite on 1/17/20.
//

#pragma once

#include <cstdint>
#include <cstring>
#include <stack>
#include <string>
#include <vector>

#include <string_view>

#include <fcntl.h>
#include <iostream>
#include <memory>
#include <sys/mman.h>
#include <zconf.h>

#define syscall_assert(expression, syscall)                                                        \
    do {                                                                                           \
        if (!(expression)) {                                                                       \
            perror(#syscall);                                                                      \
            std::terminate();                                                                      \
        }                                                                                          \
    } while (0)

namespace transaction {
    inline namespace literals {
        inline namespace byte_size_literals {
            constexpr unsigned long long operator"" _B(unsigned long long l) noexcept { return l; }
            constexpr unsigned long long operator"" _KiB(unsigned long long l) noexcept {
                return l * 1024_B;
            }
            constexpr unsigned long long operator"" _MiB(unsigned long long l) noexcept {
                return l * 1024_KiB;
            }
            constexpr unsigned long long operator"" _GiB(unsigned long long l) noexcept {
                return l * 1024_MiB;
            }
        } // namespace byte_size_literals
    }     // namespace literals
    namespace journal {
        class MmapPool {
        public:
            static constexpr std::size_t kDescriptorIndex = 0, kPointerIndex = 1;
            using ContentType = std::tuple<int, void *>;

        private:
            std::string base_directory_;
            std::size_t allocate_size_;
            std::size_t allocated_;
            std::stack<ContentType, std::vector<ContentType>> pool_;

        private:
            void allocateNew() {
                static constexpr struct { std::uint64_t _[64]; } k8BytesData = {};

                auto file_name = base_directory_ + "/journal." + std::to_string(allocated_++);
                auto fd = ::open(file_name.c_str(), O_CREAT | O_RDWR,
                                 S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
                syscall_assert(fd >= 0, open);
                for (std::size_t i = 0; i < (allocate_size_ / sizeof(k8BytesData)); ++i) {
                    syscall_assert(write(fd, &k8BytesData, sizeof(k8BytesData)) >= 0, write);
                }
                close(fd);

                fd = open(file_name.c_str(), O_RDWR);
                syscall_assert(fd >= 0, open);

                auto ptr =
                    ::mmap(nullptr, allocate_size_, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
                syscall_assert(ptr != MAP_FAILED, mmap);
                std::clog << allocate_size_ << " Bytes of journal area created on " << file_name
                          << std::endl;
                pool_.emplace(fd, ptr);
            }

        public:
            MmapPool(std::string base_directory, std::size_t allocate_size,
                     std::size_t pre_allocate_count)
                : base_directory_(std::move(base_directory))
                , allocate_size_(allocate_size)
                , allocated_(0) {
                for (std::size_t i = 0; i < pre_allocate_count; ++i) {
                    allocateNew();
                }
            }

            ~MmapPool() {
                while (!pool_.empty()) {
                    auto fp = pool_.top();
                    pool_.pop();
                    munmap(std::get<kPointerIndex>(fp), allocate_size_);
                    close(std::get<kDescriptorIndex>(fp));
                }
            }

        public:
            ContentType allocate() {
                if (pool_.empty())
                    allocateNew();
                auto data = pool_.top();
                pool_.pop();
                return data;
            }
            void deallocate(const ContentType &data) { pool_.push(data); }

        public:
            std::size_t allocate_size() const noexcept { return allocate_size_; }

        public:
            static std::shared_ptr<MmapPool> New(std::string base_directory,
                                                 std::size_t allocate_size,
                                                 std::size_t pre_allocate_count) {
                return std::make_shared<MmapPool>(std::move(base_directory), allocate_size,
                                                  pre_allocate_count);
            }
        };

        class MmapJournal {
        private:
            std::shared_ptr<MmapPool> pool_;
            int msync_flags_;
            MmapPool::ContentType area_;
            char *last_;

        public:
            explicit MmapJournal(std::shared_ptr<MmapPool> pool, bool async_commit)
                : pool_(std::move(pool))
                , msync_flags_(async_commit ? MS_ASYNC : MS_SYNC)
                , area_(pool_->allocate())
                , last_(reinterpret_cast<char *>(std::get<MmapPool::kPointerIndex>(area_))) {}

            ~MmapJournal() { pool_->deallocate(area_); }

        public:
            void append(const std::string_view &str) {
                std::memcpy(last_, str.data(), str.size());
                last_ += str.size();
                *last_ = '\0';
                last_++;
            }

            std::vector<std::string_view> get() const {
                auto first =
                    reinterpret_cast<const char *>(std::get<MmapPool::kPointerIndex>(area_));

                std::vector<std::string_view> contents;

                while (first < last_) {
                    contents.emplace_back(first);
                    first += contents.back().size() + 1;
                }

                ::msync(std::get<MmapPool::kPointerIndex>(area_), pool_->allocate_size(),
                        msync_flags_);

                return contents;
            }
        };
    } // namespace journal
} // namespace transaction
