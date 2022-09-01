//
// Created by mint25mt on 2022/08/23.
//

#ifndef MY_PROXY_REQUEST_H
#define MY_PROXY_REQUEST_H
#include <string>
#include <unordered_map>
#include <utility>
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
                , _peer(std::move(p)) {
//            std::cout << "query.size()" << _queries.size() << std::endl;
//            std::cout << "req.query() is : " << _queries[0].query() << std::endl; // ここはOK
        }
        Request(Peer p, Query query)
                : Request(std::move(p), std::vector<Query>{query}) {
        }

        Request(Peer p, std::string_view query)
                : Request(std::move(p), Query(custom_search::query_itr(query))) { // dataで渡すと動くう
            std::cout << "raw_request: " << query << std::endl; // ここはいける

            std::cout << "trimed :"  << custom_search::query_itr(query) << std::endl;
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
//            std::cout << "query.size()" << _queries.size() << std::endl;
//            std::cout << "req.query()" << _queries[0].query() << std::endl;

            return _queries[0]; }
        [[nodiscard]] const Peer &peer() const { return _peer; }
    };
}

#endif //MY_PROXY_REQUEST_H
