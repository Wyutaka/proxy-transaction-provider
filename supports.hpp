//
// Created by MiyaMoto on 2019-07-18.
//

#ifndef TRANSACTION_SUPPORTS_HPP
#define TRANSACTION_SUPPORTS_HPP

#include <algorithm>
#include <cstdint>
#include <limits>
#include <random>
#include <stdexcept>

namespace transaction {
    namespace pipes {
        template <class T> using Vector = std::vector<T>;

        template <class Container, class Function, template <class> class RetContainer = Vector>
        auto operator|(Container c, Function f) -> RetContainer<decltype(f(*std::begin(c)))> {
            RetContainer<decltype(f(*std::begin(c)))> ret(c.size());
            std::transform(std::begin(c), std::end(c), std::begin(ret), f);
            return ret;
        }
        template <class Function> std::string operator|(std::string c, Function f) {
            std::string ret(c.size(), 0);
            std::transform(std::begin(c), std::end(c), std::begin(ret), f);
            return ret;
        }
    } // namespace pipes

    bool StartsWith(const std::string &target, const std::string &check) {
        return target.find(check) == 0;
    }
} // namespace transaction

#endif // TRANSACTION_SUPPORTS_HPP
