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

std::cmatch first_roman_itr(std::string_view corpus) {
    std::cout << "corpus : " << corpus << std::endl; // これは出力される
    std::cmatch m;
    std::regex_search(corpus.begin(), corpus.end(), m, std::regex("[A-Za-z]"));
    return m;
}

std::string_view query_itr(std::string_view corpus) {
    // DONE A.SELECTの場合、Aは取り除きたい
    // TODO 後ろの余分な文字を切り取りたい?
    // 末尾に$がある
    std::cout << "corpus : " << corpus << std::endl; // これは出力される
    std::cmatch m;
    int result = std::regex_search(corpus.begin(), corpus.end(), m, std::regex("(be|co|ro|in|se|up|cr)\.\+", std::regex_constants::icase));
    std::string::size_type semi_pos = m.str().find(';');
    if (result == 1) {
        std::cout << "pattern match " << std::endl; // これは出力される
        std::cout << "str = " << m.str() << "\nsubmatch_size = " << m.size()  << "\nstr_size = " << m.str().size() << "\nposition = " << m.position() << std::endl;
        return corpus.substr(m.position(), semi_pos) ;
    } else {
        return corpus;
    }

}

int main() {
    size_t pos;
//    std::string corpus = "BANNANABANANAN";
//    std::string start_pattern = "BANANA";

    std::string_view corpus = "4SELECT * FROM ybdemo.employee;4�U|��$��Tz��";
    std::string start_pattern = "SELECT";
    // a-z,A-Zを切り抜きたい


    // (2) の形式
//    std::cmatch m = query_itr(corpus);
//    std::cout << "str = '" << m.str() << "', position = " << m.position() << std::endl;
//    std::cout << std::string_view(corpus).substr(m.position(), corpus.size()) << std::endl;
    std::cout << query_itr(corpus) << std::endl;

//    std::pair search_result = boost::algorithm::boyer_moore_search(
//            corpus.begin(), corpus.end(),
//            start_pattern.begin(), start_pattern.end());
//
//    std::cout << (search_result.first - corpus.begin()) << std::endl;
    return 0;
}

