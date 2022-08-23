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
                : Request(std::move(p), std::vector<Query>{std::move(query)}) {}

        Request(Peer p, std::string_view query)
                : Request(std::move(p), Query(std::move(query))) {}

    public:
        [[nodiscard]] auto begin() { return std::begin(_queries); }
        [[nodiscard]] auto end() { return std::end(_queries); }
        [[nodiscard]] auto begin() const { return std::begin(_queries); }
        [[nodiscard]] auto end() const { return std::end(_queries); }

    public:
        [[nodiscard]] const std::vector<Query> &queries() const { return _queries; }
        [[nodiscard]] const Query &query() const { return _queries[0]; }
        [[nodiscard]] const Peer &peer() const { return _peer; }
    };
}

#endif //MY_PROXY_REQUEST_H
