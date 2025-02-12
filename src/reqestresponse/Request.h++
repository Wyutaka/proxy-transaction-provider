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
#include "../test/DumpHex.h++"

namespace transaction {
    using PeerV4 = BasicPeer<std::string>;
    using Peer = PeerV4;

    class Request {
    private:
        std::vector<Query> _queries;
        Peer _peer;
        std::string _raw_request;

    public:
        explicit Request(Peer p, std::vector<Query> queries, std::string_view raw_request)
                : _queries(std::move(queries))
                , _peer(std::move(p))
                , _raw_request(raw_request) {
//            std::cout << "raw_request in req req: " << std::endl; // ここはいける
//            debug::hexdump(_raw_request.data(), _raw_request.size());
        }
        Request(Peer p, Query query, std::string_view raw_request)
                : Request(std::move(p), std::vector<Query>{query}, raw_request) {
        }

        Request(Peer p, std::string_view query_body, std::string_view raw_request)
                : Request(std::move(p), Query(query_body), raw_request) {}

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
//            std::cout << "in Request req.query()" << _queries[0].query() << std::endl;

            return _queries[0]; }
        [[nodiscard]] std::string raw_request() const {
            return _raw_request; }
        [[nodiscard]] const Peer &peer() const { return _peer; }
    };
}

#endif //MY_PROXY_REQUEST_H
