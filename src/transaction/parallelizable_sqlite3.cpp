//
// Created by mint25mt on 2022/08/23.
//

//
// Created by cerussite on 2019/12/21.
//

#pragma once

#include <map>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "../../transaction_provider.h++"
#include "parallelizable_sqlite3.hpp"

namespace transaction {
    namespace detail {
        class State {
        public:
            State::State()
                    : _s3(nullptr) {
                sqlite3_open_v2(":memory:", &_s3, SQLITE_OPEN_READWRITE, nullptr);
            }

            State::State(const State &) = delete;
            State::State(State &&rhs) noexcept
                    : _s3(rhs._s3)
                    , _queries(std::move(rhs._queries)) {
                rhs._s3 = nullptr;
            }

            State::State &operator=(const State &) = delete;
            State::State &operator=(State &&rhs) noexcept {
                reset();
                using std::swap;
                swap(_s3, rhs._s3);

                _queries = std::move(rhs._queries);

                return *this;
            }

            State::~State() { reset(); }

            void State::reset() noexcept {
                if (_s3)
                    sqlite3_close_v2(_s3);
                _s3 = nullptr;
            }

            bool State::executeOnly(const Query &query) noexcept {
                char *errMsg = nullptr;

                auto res =
                        sqlite3_exec(_s3, query.query().data(), nullptr, nullptr, &errMsg);

                return res == SQLITE_OK;
            }

            void State::addQueryOnly(const Query &query) { _queries.emplace_back(query); }

            bool State::add(const Query &query) {
                addQueryOnly(query);
                return executeOnly(query);
            }

            const std::vector<Query> &State::getAllQueries() const { return _queries; }
        };
    } // namespace detail

    template <class NextF>
    class ParallelizableSqlite3TransactionProvider : public TransactionProvider<NextF> {
    private:
        std::map<Peer, detail::State> _states;

    public:
        using TransactionProvider<NextF>::TransactionProvider;

    public:
        Response begin(const Request &req) override {
            _states.emplace(req.peer(), std::move(detail::State()));
            return {CoResponse(Status::Ok)};
        }

        Response commit(const Request &req) override {
            const auto &state = _states[req.peer()];
            const auto &queries = state.getAllQueries();

            auto res = this->next(Request(req.peer(), queries));
            _states.erase(req.peer());
            return res;
        }

        Response rollback(const Request &req) override {
            _states.erase(req.peer());
            return {CoResponse(Status::Ok)};
        }

        Response query(const Request &req) override {
            auto itr = _states.find(req.peer());
            if (itr == std::end(_states)) {
                return this->next(req);
            }

            itr->second.add(req.query());
            return {CoResponse(Status::Ok)};
        }
    };
} // namespace transaction
