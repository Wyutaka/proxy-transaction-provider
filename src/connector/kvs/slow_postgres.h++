//
// Created by user1 on 22/10/20.
//

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
#include "../../transaction/result.h++"


#define CASS_SHARED_PTR(type, v)                                                                   \
    std::shared_ptr<std::remove_pointer_t<decltype(v)>>(v, [](decltype(v) t) {                     \
        if (t)                                                                                     \
            cass_##type##_free(t);                                                                 \
    })

namespace transaction {

    class CassandraConnectorPGSQL {
    private:
        boost::shared_ptr<tcp_proxy::bridge> _bridge;
        std::shared_ptr<CassFuture> _connectFuture;
        std::shared_ptr<CassCluster> _cluster;
        std::shared_ptr<CassSession> _session;

    public:
        explicit CassandraConnectorPGSQL(boost::shared_ptr<tcp_proxy::bridge> bridge,
                                         std::shared_ptr<CassCluster> cluster, std::shared_ptr<CassSession> session)
                : _connectFuture(), _cluster(cluster), _session(session), _bridge(bridge) {

                }


        ~CassandraConnectorPGSQL() {
//            _connectFuture.reset();
//            _session.reset();
//            _cluster.reset();
        }

    private:

        template <class C>
        void print(const C& c, std::ostream& os = std::cout)
        {
            std::for_each(std::begin(c), std::end(c), [&os](typename C::value_type p) { os << '{' << p.first << ',' << &p.second << "}, "; });
            os << std::endl;
        }

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
                    std::cout << valueStr << std::endl;

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
                    std::cout << i32 << std::endl;

                    return RowData(i32);
                }
                case CASS_VALUE_TYPE_BIGINT:  // i64
                case CASS_VALUE_TYPE_COUNTER: // 64
                {
                    std::int64_t i64;
                    cass_value_get_int64(value, &i64);

                    return RowData(i64);
                }
                default:
                    // not supported data types
                    return RowData(); // unknown
            }
        }

    public:
        // ここをプロキシ(bridge)に置き換える
        Response operator()(const Request &req) {
            const auto &raw_request = req.raw_request();

            std::vector<std::shared_ptr<CassFuture>> resultFutures;     // CassFuture(実行結果)のsharedptrのベクタを定義
            resultFutures.reserve(req.queries().size());                // クエリサイズ分予約
//            resultFutures.reserve(req.query().query().size());                // クエリサイズ分予約
//            for (const auto &query : req.queries()) {                   // クエリの数だけ(req.queriesが正しく値が取れていないせつ)
//            debug::hexdump(reinterpret_cast<const char *>(req.query().query().data()), req.query().query().size()); // 下流バッファバッファ16進表示

            std::cout << req.query().query().data() << std::endl;
            std::cout << "req.query.size::" << req.queries().size() << std::endl;
            std::cout << "req.query.size::" << req.query().query().size() << std::endl;
            CassStatement* statement = cass_statement_new(req.query().query().data(), 0); // statement, cass_statement_new_n(const char * query, size_t query_length, size_t parameter_count)

            auto result_future = cass_session_execute(_session.get(), statement);

            if (req.query().isInsert()) {
                std::cout << "insert" << std::endl;
                return std::vector<CoResponse>{CoResponse(Status::Ok)};
            }
//            }

            Response res({CoResponse(Status::Pending)});                                               // response

//            res.reserve(req.queries().size());                          // クエリの数分responseの要素を予約
            res.reserve(req.query().query().size());                          // クエリの数分responseの要素を予約()


//            for (auto &future_: resultFutures) {
            for (int i = 0; i < req.queries().size(); i++) {

                auto future = result_future;                            // get():保持しているポインタを取得

                auto errorCode = cass_future_error_code(future);        // 実行結果をエラーコード判定にかける
                if (errorCode != CASS_OK) {
                    std::cerr << errorCode << " " << cass_error_desc(errorCode) << std::endl;
                    /*if (auto errorResult =
                            CASS_SHARED_PTR(error_result, cass_future_get_error_result(future));
                        errorResult) {
                    }*/
                    std::cout << "backend Err:::" << cass_error_desc(errorCode) << std::endl;
                    return std::vector<CoResponse>{CoResponse(Status::Error)};
                }

                auto result = CASS_SHARED_PTR(result, cass_future_get_result(future));      // 成功の結果を取得(futureを待機) TODO ここで待つから遅い
                if (!result) {
                    std::cerr << "get result failed" << std::endl;
                    return std::vector<CoResponse>{CoResponse(Status::Error)};
                }
                auto columnCount = cass_result_column_count(result.get());                  // 指定された結果の 1 行あたりの列数を取得
                if (columnCount == 0) {
                    return std::vector<CoResponse>{CoResponse(Status::Ok)};
                }
                std::cout << "column size" << columnCount << std::endl;
                std::vector<std::string> columnNames;
                for (std::size_t cn = 0; cn < columnCount; ++cn) {
                    std::size_t length = 0;                                   // 列名の長さ
                    const char *nbp = nullptr;                                // 列名
                    cass_result_column_name(result.get(), cn, &nbp, &length); // 指定された結果のインデックスで列名を取得します。(const CassResult * result, size_t index, const char ** name, size_t * name_length)
                    std::cout << nbp << std::endl;                            // 列名をprintデバッグ
                    columnNames.emplace_back(std::string(nbp, nbp + length)); // columnNamesの末尾に列名を追加(文字列の範囲から構築)
                }

                auto iterator = CASS_SHARED_PTR(iterator, cass_iterator_from_result(result.get()));  // 指定された結果の新しいイテレータ.結果の行を反復処理するために使用.


                std::vector<Row> rows;
                response::Sysbench backend_res({"pk", "field1", "field2", "field3"}, 4);
                while (cass_iterator_next(iterator.get())) {                 // イテレータの次の値が存在するまでイテレータを次の行、列、またはコレクション アイテムに進める
                    Row resRow;
                    auto row = cass_iterator_get_row(iterator.get());        // resultイテレータの現在の位置にある行を取得。
                    if (row) {
                        for (std::size_t i = 0; i < columnNames.size(); ++i) {   // カラム名の数だけ
                            auto value = cass_row_get_column(row, i);            // 指定された行の index にある列の値を取得 // type->CassValue : 指定されたインデックスの列の値。インデックスが範囲外の場合は NULL が返されます。
                            resRow[columnNames[i]] = _toRowData(value);          // valueをRowDataに変換
                        }
                        rows.emplace_back(resRow); // resRowをrowsに末尾に追加
                    }
                }
                backend_res.addRow("hoge", 1, 2, 3);
//                std::string str(backend_res.bytes().begin(), backend_res.bytes().end());
                debug::hexdump(reinterpret_cast<const char *>(backend_res.bytes().data()), backend_res.bytes().size());

                // rowからで０で
//
//                res.emplace_back(Status::Result, rows);                      // resの末尾にResult,rowを追加
            }

            return res;
        }
    };
} // namespace transaction

#undef CASS_SHARED_PTR

