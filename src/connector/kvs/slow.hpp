
#pragma once

#include "../../../kvs.h++"
#include <cassandra.h>
#include <iostream>
#include <memory>
#include "../../../server.h++"
#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include <utility>
#include "server.h++"
#include <stdlib.h>
#include <iostream>
#include "./src/lock/Lock.h++"
#include "./src/transaction/transaction_impl.hpp"
#include "./src/test/DumpHex.h++"

#define CASS_SHARED_PTR(type, v)                                                                   \
    std::shared_ptr<std::remove_pointer_t<decltype(v)>>(v, [](decltype(v) t) {                     \
        if (t)                                                                                     \
            cass_##type##_free(t);                                                                 \
    })

namespace transaction {

    namespace detail {
        struct SessionDeleter {
            void operator()(CassSession *s) const noexcept {
                {
                    auto close_future = CASS_SHARED_PTR(future, cass_session_close(s));
                    cass_future_wait(close_future.get());
                }
                cass_session_free(s);
            }
        };
    } // namespace detail

    class SlowCassandraConnector {
    private:
        boost::shared_ptr<tcp_proxy::bridge> _bridge;
        std::shared_ptr<CassFuture> _connectFuture;
        std::shared_ptr<CassCluster> _cluster;
        std::shared_ptr<CassSession> _session;

    public:
        explicit SlowCassandraConnector(const std::string &host, boost::shared_ptr<tcp_proxy::bridge> bridge)
            // moveじゃなくない？
            : _connectFuture()
            , _cluster(CASS_SHARED_PTR(cluster, cass_cluster_new()))
            , _session(std::shared_ptr<CassSession>(cass_session_new(), detail::SessionDeleter()))
            , _bridge(bridge) {
            cass_cluster_set_contact_points(_cluster.get(), host.c_str());
            _connectFuture =
                    CASS_SHARED_PTR(future, cass_session_connect(_session.get(), _cluster.get()));

            if (cass_future_error_code(_connectFuture.get()) != CASS_OK) {
                std::cerr << "cannot connect to Cassandra" << std::endl;
                std::terminate();
            }

        }

        ~SlowCassandraConnector() {
//            _connectFuture.reset();
//            _session.reset();
//            _cluster.reset();
        }

    private:
        static RowData _toRowData(const CassValue *value) {
            auto dataType = cass_value_data_type(value);
            auto valueType = cass_data_type_type(dataType);
            switch (valueType) {
            case CASS_VALUE_TYPE_ASCII:
            case CASS_VALUE_TYPE_VARCHAR:
            case CASS_VALUE_TYPE_TEXT: {
                const char *valueStr = nullptr;
                std::size_t size = 0;

                cass_value_get_string(value, &valueStr, &size);

                return RowData(std::string(valueStr, valueStr + size));
            }
            case CASS_VALUE_TYPE_TINY_INT: // i8
            {
                std::int8_t i8;
                cass_value_get_int8(value, &i8);

                return RowData(i8);
            }
            case CASS_VALUE_TYPE_SMALL_INT: // i16
            {
                std::int16_t i16;
                cass_value_get_int16(value, &i16);

                return RowData(i16);
            }
            case CASS_VALUE_TYPE_INT: // i32
            {
                std::int32_t i32;
                cass_value_get_int32(value, &i32);

                return RowData(i32);
            }
            case CASS_VALUE_TYPE_BIGINT:  // i64
            case CASS_VALUE_TYPE_COUNTER: // 64
            {
                std::int64_t i64;
                cass_value_get_int64(value, &i64);

                return RowData(i64);
            }

            case CASS_VALUE_TYPE_DECIMAL:
            case CASS_VALUE_TYPE_VARINT:
            case CASS_VALUE_TYPE_BLOB:
            case CASS_VALUE_TYPE_UNKNOWN:
            case CASS_VALUE_TYPE_CUSTOM:
            case CASS_VALUE_TYPE_DOUBLE:
            case CASS_VALUE_TYPE_FLOAT:
            case CASS_VALUE_TYPE_LIST:
            case CASS_VALUE_TYPE_MAP:
            case CASS_VALUE_TYPE_SET:
            case CASS_VALUE_TYPE_UDT:
            case CASS_VALUE_TYPE_TUPLE:
            case CASS_VALUE_TYPE_LAST_ENTRY:
            case CASS_VALUE_TYPE_DURATION:
            case CASS_VALUE_TYPE_TIMEUUID:
            case CASS_VALUE_TYPE_INET:
            case CASS_VALUE_TYPE_DATE:
            case CASS_VALUE_TYPE_TIME:
            case CASS_VALUE_TYPE_TIMESTAMP:
            case CASS_VALUE_TYPE_UUID:
            case CASS_VALUE_TYPE_BOOLEAN:
            default:
                // not supported data types
                return RowData(); // unknown
            }
        }

    public:
        // ここをプロキシ(bridge)に置き換える
        Response operator()(const Request &req, sqlite3 *in_mem_db) {
//            const auto& raw_request = req.raw_request();
//            std::cout << "raw_request: " << req.query().query() << std::endl;
//            std::cout << "transaction" << std::endl;
//            debug::hexdump(raw_request.data(), raw_request.size());

            // サーバーに送信
//            async_write(_bridge->upstream_socket_,
//                        boost::asio::buffer(raw_request.data(), raw_request.size()),
//                        boost::bind(&tcp_proxy::bridge::handle_upstream_write,
//                                    _bridge,
//                                    boost::asio::placeholders::error));


//            resultFutures.reserve(req.queries().size());
//            for (const auto &query : req.queries()) {
//
//                auto statement = CASS_SHARED_PTR(
//                    statement, cass_statement_new_n(query.query().data(), query.query().size(), 0));
//                resultFutures.emplace_back(
//                    CASS_SHARED_PTR(future, cass_session_execute(_session.get(), statement.get()))); // execute
//            }

            Response res = Response({CoResponse(Status::Pending)});
            // todo クエリ,成功ごとにレスポンスを分ける(Ok, result,pending)
//            res.reserve(req.queries().size()); // responseのサイズ変更通知
//            for (auto &future_ : resultFutures) {
//                auto future = future_.get();
//
//                auto errorCode = cass_future_error_code(future);
//                if (errorCode != CASS_OK) {
//                    std::cerr << errorCode << " error!!! " << cass_error_desc(errorCode) << std::endl;
//                    /*if (auto errorResult =
//                            CASS_SHARED_PTR(error_result, cass_future_get_error_result(future));
//                        errorResult) {
//                    }*/
//                    res.emplace_back(Status::Error);
//                    continue;
//                }
//                auto result = CASS_SHARED_PTR(result, cass_future_get_result(future));
//                if (!result) {
//                    std::cerr << "get result failed" << std::endl;
//                    res.emplace_back(Status::Error);
//                    continue;
//                }
//
//                auto columnCount = cass_result_column_count(result.get());
//                if (columnCount == 0) {
//                    res.emplace_back(Status::Ok);
//                    continue;
//                }
//                auto iterator = CASS_SHARED_PTR(iterator, cass_iterator_from_result(result.get()));
//
//                std::vector<std::string> columnNames;
//                for (std::size_t cn = 0; cn < columnCount; ++cn) {
//                    std::size_t length = 0;
//                    const char *nbp = nullptr;
//                    cass_result_column_name(result.get(), cn, &nbp, &length);
//
//                    columnNames.emplace_back(std::string(nbp, nbp + length));
//                }
//
//                std::vector<Row> rows;
//                while (cass_iterator_next(iterator.get())) {
//                    Row resRow;
//                    auto row = cass_iterator_get_row(iterator.get());
//                    for (std::size_t i = 0; i < columnNames.size(); ++i) {
//                        auto value = cass_row_get_column(row, i);
//                        resRow[columnNames[i]] = _toRowData(value);
//                    }
//                    rows.emplace_back(resRow);
//                }
//
//                res.emplace_back(Status::Result, rows);
//            }

            return res;
        }
    };
} // namespace transaction

#undef CASS_SHARED_PTR
