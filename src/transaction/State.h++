//
// Created by mint25mt on 2022/08/23.
//

#ifndef MY_PROXY_STATE_H
#define MY_PROXY_STATE_H

#include <map>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "../../transaction_provider.h++"

namespace transaction {
    namespace detail {
        class State {

        public:
            State();
            State(const State &) = delete;
            State(State &&rhs) noexcept;
            State &operator=(const State &);
            State &operator=(State &&rhs) noexcept;
            ~State();

            bool executeOnly(const Query &query) noexcept;
            void addQueryOnly(const Query &query);
            bool add(const Query &query);
            const std::vector <Query> &getAllQueries() const;

        private:
            sqlite3 *_s3;
            std::vector <Query> _queries;

            void reset() noexcept;
        };
    } // namespace detail

}
#endif //MY_PROXY_STATE_H
