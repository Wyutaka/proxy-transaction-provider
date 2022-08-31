//
// Created by user1 on 22/08/31.
//
#pragma once

//
// Created by user1 on 22/08/30.
//

#include <boost/version.hpp>
#include <iostream>
#include <iomanip>
#include <string>
#include <string_view>
#include <iostream>
#include <iterator>
#include <list>
#include <regex>
#include <string>
#include <boost/algorithm/searching/boyer_moore.hpp>
#include <boost/algorithm/searching/boyer_moore_horspool.hpp>
#include <boost/algorithm/searching/knuth_morris_pratt.hpp>

namespace custom_search {
    std::cmatch first_roman(std::string_view corpus) {
        std::cout << "corpus : " << corpus << std::endl; // これは出力される
//        std::cout << "corpus data : " << corpus.data() << std::endl; // null

        std::cmatch m;
        int result = std::regex_search(corpus.begin(), corpus.end(), m, std::regex("[A-Za-z]")); //
//        int result = std::regex_search(corpus.data(), m, std::regex("[A-Za-z]")); //

        if (result == 1) {
            std::cout << "pattern found" << std::endl;
        }
        return m;
    }
} // namespace custom_search




