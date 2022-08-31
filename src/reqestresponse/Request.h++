//
// Created by mint25mt on 2022/08/23.
//

#ifndef MY_PROXY_REQUEST_H
#define MY_PROXY_REQUEST_H
#include <string>
#include <unordered_map>
#include <vector>
#include <string_view>

#include "query.h++"
#include "RowData.h++"
#include "CoResponse.h++"
#include <boost/algorithm/string/replace.hpp>
#include <any>
#include "src/test/search.h++"
#include <regex>


namespace transaction {
    using PeerV4 = BasicPeer<std::string>;
    using Peer = PeerV4;

    class Request {
    private:
        std::vector<Query> _queries;
        Peer _peer;

    public:
        explicit Request(Peer p, std::vector<Query> queries)
                : _queries(std::move(queries))
                , _peer(std::move(p)) {}
        Request(Peer p, Query query)
                : Request(std::move(p), std::vector<Query>{query}) {
        }

        Request(Peer p, std::string_view query)
                : Request(std::move(p), Query(query.substr(custom_search::first_roman(query).position(), query.size()))) { // dataで渡すと動くう
            std::cout << "raw_request: " << query << std::endl; // ここはいける

            std::cmatch m = custom_search::first_roman(std::string(query)); // queryがfirst_roman側でからもじ
            std::cout << "str = '" << m.str() << "', position = " << m.position() << std::endl;
            std::cout << "trimed :"  << query.substr(m.position(), query.size()) << std::endl;
        }

    public:
        [[nodiscard]] auto begin() { return std::begin(_queries); }
        [[nodiscard]] auto end() { return std::end(_queries); }
        [[nodiscard]] auto begin() const { return std::begin(_queries); }
        [[nodiscard]] auto end() const { return std::end(_queries); }

    public:
//        [[nodiscard]] const std::vector<Query> &queries() const { return _queries; }
        [[nodiscard]] const std::vector<Query> queries() const { return _queries; }
        [[nodiscard]] const Query &query() const {
            // 正しい値が遅れない
//            std::cout << "req.query()" << _queries[0].query() << std::endl; // ここはだめ

            return _queries[0]; }
        [[nodiscard]] const Peer &peer() const { return _peer; }
    };
}

#endif //MY_PROXY_REQUEST_H