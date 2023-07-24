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
#include "detail.h++"
#include "shared_mutex.h++"
#include <fstream>

namespace transaction::lock {
        template<class NextF> class Lock {
        private:
            using Ul = std::unique_lock<detail::shared_mutex>;
            using Sl = detail::shared_lock<detail::shared_mutex>;
            NextF _next; // transaction
            detail::MyAtomicFlag _lock;
            std::unique_ptr<detail::shared_mutex> _mutex;
            Peer _currentLocker;

            bool _getLock(const Peer &peer) {
                // ip port

                if (_lock.test_and_set() == true) {
                    return false;
                }
                Ul ul(*_mutex);
                _currentLocker = peer;
                return true;
            }

            void write_query_to_wal(const std::basic_string<char>& wal_name, std::string_view query) {
                std::ofstream writing_file;
                writing_file.open(wal_name, std::ios::app);
                writing_file << query << std::endl;
                writing_file.close();
            }

        public:
            explicit Lock(NextF next)
                    : _next(std::move(next)), _mutex(std::make_unique<detail::shared_mutex>()) {}

//            Response operator()(const Request &req);

            Response operator()(const Request &req, std::string_view wal_file_name, std::queue<std::string> &query_queue, sqlite3 *in_mem_db) {
                using pipes::operator|;
                
                std::cout << req.query().query() << std::endl;
            // const auto query = req.query() | tolower;

                if (req.query().isBegin()) {
                if (!_getLock(req.peer())) {
                    return Response({CoResponse(Status::Error)});
                } else {
//                    std::cout << "Lock succeed" << std::endl;
                    return Response({CoResponse(Status::Ok)});
                }
            } else if (req.query().isInsertIfNotExists() || req.query().isCommit() || req.query().isRollback()) {
                if (!_getLock(req.peer()) && req.query().isInsertIfNotExists()) {
                    return Response({CoResponse(Status::Error)});
                }
                _lock.clear();
                return Response({CoResponse(Status::Commit)});
            } else if (_lock.test()) {
                Sl sl(*_mutex);
                if (req.peer() != _currentLocker) {
                    return Response({CoResponse(Status::Error)});
                }
            }
            write_query_to_wal(wal_file_name.data(), req.query().query().data());
            if (!req.query().isSelect()) {
                query_queue.push(req.query().query().data());
            }
            return _next(req, in_mem_db);
            }

        };
    }
#endif //MY_PROXY_LOCK_H
