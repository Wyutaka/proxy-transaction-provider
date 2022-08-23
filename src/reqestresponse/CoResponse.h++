//
// Created by mint25mt on 2022/08/23.
//

#ifndef MY_PROXY_CORESPONSE_H
#define MY_PROXY_CORESPONSE_H

#include <string>
#include <unordered_map>
#include <vector>
#include <string_view>

#include "query.h++"
#include "src/reqestresponse/RowData.h++"
#include "src/peer/Peer.hpp"
#include <boost/algorithm/string/replace.hpp>
#include <any>
#include "../peer/Peer.hpp"

namespace transaction {

    using Row = std::unordered_map<std::string, RowData>;

    // TODO Peer.hppで定義されてるからいらないかも
    enum class Status {
        Error = 0,
        Ok,
        Result,
    };

    class CoResponse {
    private:
        Status _status;
        std::vector<Row> _data;

    public:
        CoResponse() = default;

        CoResponse(Status status, std::vector<Row> data)
                : _status(status), _data(std::move(data)) {}

        explicit CoResponse(Status status)
                : CoResponse(status, {}) {}

    public:
        [[nodiscard]] Status status() const noexcept { return _status; }

        [[nodiscard]] const std::vector<Row> &data() const { return _data; }
    };

    using Response = std::vector<CoResponse>;
}
#endif //MY_PROXY_CORESPONSE_H
