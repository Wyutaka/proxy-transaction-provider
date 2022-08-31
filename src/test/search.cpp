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

std::cmatch first_roman(std::string_view corpus) {
    std::cmatch m;
//    std::cout << "(2) " << std::regex_search(corpus.c_str(), m, sttd::regex("[A-Za-z]+")) << std::endl;
    int result = std::regex_search(corpus.data(), m, std::regex("[A-Za-z]"));
    if (result == 1) {
        std::cout << "pattern found" << std::endl;
    }
//    std::cout << "str = '" << m.str() << "', position = " << m.position() << std::endl;
    return m;
}

std::cmatch wrap(std::string_view corpus) {
    return first_roman(corpus);
}

int main() {
    size_t pos;
//    std::string corpus = "BANNANABANANAN";
//    std::string start_pattern = "BANANA";

//    std::string corpus = "!SELECT * FROM system.peers";
    std::string_view corpus = "!SELECT * FROM system.peers";

    std::string start_pattern = "SELECT";
    // a-z,A-Zを切り抜きたい

    // (2) の形式
    std::cmatch m = wrap(corpus);
//    std::cout << "(2) " << std::regex_search(corpus.c_str(), m, std::regex("[A-Za-z]+")) << std::endl;
    std::cout << "str = '" << m.str() << "', position = " << m.position() << std::endl;
    std::cout << std::string_view(corpus).substr(m.position(), corpus.size()) << std::endl;


    std::pair search_result = boost::algorithm::boyer_moore_search(
            corpus.begin(), corpus.end(),
            start_pattern.begin(), start_pattern.end());

    std::cout << (search_result.first - corpus.begin()) << std::endl;
    return 0;
}

