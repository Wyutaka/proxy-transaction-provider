//
// Created by mint25mt on 2022/08/23.
//

#ifndef MY_PROXY_CORESPONSE_H
#define MY_PROXY_CORESPONSE_H

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <string_view>
#include <variant>

#include "query.h++"
#include "src/reqestresponse/RowData.h++"
#include "src/peer/Peer.hpp"
#include <boost/algorithm/string/replace.hpp>
#include <any>
#include "../peer/Peer.hpp"
#include "src/transaction/result.h++"

namespace transaction {

    using Row = std::unordered_map<std::string, RowData>; // row_name , data

    enum class Status {
        Error = 0,
        Ok,
        Result, // 内部DBから結果を取得済み
        Pending, // 非同期にバックエンドに送信中
        Select_Pending, // 内部DBに結果がない
        Commit,
    };

    class CoResponse {
    private:
        Status _status;
        std::vector<Row> _data;
//        std::string raw_response;
//        unsigned char* raw_response;
        std::queue<std::variant<response::Sysbench, response::Sysbench_one, response::Sysbench_sum>> _results;
        std::vector<unsigned char> raw_response;

    public:
        CoResponse() = default;

        CoResponse(Status status, std::vector<Row> data)
                : _status(status), _data(std::move(data)) {}

        explicit CoResponse(Status status)
                : CoResponse(status, {}) {}

    public:
        [[nodiscard]] Status status() const noexcept { return _status; }

        void set_status(Status status) {
            _status = status;
        }

        void set_raw_response(std::vector<unsigned char> bytes) {
            std::cout << "set_raw_response" << std::endl;
            raw_response = std::move(bytes);
            std::cout << "std::move" << std::endl;

        }

        void set_results(std::queue<std::variant<response::Sysbench, response::Sysbench_one, response::Sysbench_sum>> &results) {
            _results = std::move(results);
        }

        [[nodiscard]] std::vector<unsigned char> get_raw_response() const {return raw_response;}

        [[nodiscard]] const std::vector<Row> &data() const { return _data; }

        [[nodiscard]] std::queue<std::variant<response::Sysbench, response::Sysbench_one, response::Sysbench_sum>> get_results() const {return _results;}
    };

    using Response = std::vector<CoResponse>;
}
#endif //MY_PROXY_CORESPONSE_H
