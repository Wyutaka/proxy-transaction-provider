//
// Created by mint25mt on 2022/08/23.
//
#pragma once
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
        std::queue<Query> _queries_queue;
        Peer _peer;
        std::queue<unsigned char> _cliend_queue;
        // クライアントに返すカラムの書式コード(クエリ毎に持ち、1クエリに対してstd::queue<int>が割り当てられる)
        std::queue<std::queue<int>> _column_format_codes;
        unsigned char& _transaction_state;
//        std::string _raw_request;

    public:
        explicit Request(Peer p, std::vector<Query> queries, std::queue<unsigned char> client_queue)
                : _queries(std::move(queries))
                , _peer(std::move(p))
                , _cliend_queue(std::move(client_queue)) {}

        Request(Peer p, Query query, std::queue<unsigned char> client_queue)
                : Request(std::move(p), std::vector<Query>{query}, std::move(client_queue)) {}

        Request(Peer p, std::queue<Query> queries, std::queue<unsigned char> client_queue,
                std::queue<std::queue<int>> column_format_codes,
                unsigned char& transaction_state
                )
                : Request(std::move(p), queries.front(), std::move(client_queue), transaction_state) {
            _queries_queue = queries;
            _column_format_codes = std::move(column_format_codes);
            _transaction_state = transaction_state;
        }

    public:
        [[nodiscard]] auto begin() { return std::begin(_queries); }
        [[nodiscard]] auto end() { return std::end(_queries); }
        [[nodiscard]] auto begin() const { return std::begin(_queries); }
        [[nodiscard]] auto end() const { return std::end(_queries); }

    public:
        [[nodiscard]] const std::vector<Query> queries() const { return _queries; }
        [[nodiscard]] const std::queue<Query> queryQueue() const {return _queries_queue;}
        void addQuery(std::string_view query) { _queries_queue.push(Query(query)); }
        [[nodiscard]] const Query &query() const {
            return _queries[0]; }
        [[nodiscard]] const Peer &peer() const { return _peer; }
        [[nodiscard]] const std::queue<unsigned char> client_queue() const { return _cliend_queue; }
        const std::queue<std::queue<int>> column_format_codes() const {return _column_format_codes; }
        void set_transaction_state(unsigned char new_state) {_transaction_state = new_state;}
        [[nodiscard]] const unsigned char get_transaction_state() const  { return _transaction_state; }

    };
}

