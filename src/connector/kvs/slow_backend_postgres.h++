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
#include <libpq-fe.h>
#include "./src/lock/Lock.h++"
#include "./src/transaction/transaction_impl.hpp"
#include "./src/test/DumpHex.h++"

#define CASS_SHARED_PTR(type, v)                                                                   \
    std::shared_ptr<std::remove_pointer_t<decltype(v)>>(v, [](decltype(v) t) {                     \
        if (t)                                                                                     \
            cass_##type##_free(t);                                                                 \
    })


static void
exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

namespace transaction {

    class CassandraConnectorBAKSQL {
    private:
        boost::shared_ptr<tcp_proxy::bridge> _bridge;
        std::shared_ptr<CassFuture> _connectFuture;
        PGconn* _conn;

    public:
        explicit CassandraConnectorBAKSQL(boost::shared_ptr<tcp_proxy::bridge> bridge, PGconn* conn)
                : _connectFuture(), _conn(conn), _bridge(bridge) {

        }


        ~CassandraConnectorBAKSQL() {
//            _connectFuture.reset();
//            _session.reset();
//            _cluster.reset();
        }

    private:

    public:
        // ここをプロキシ(bridge)に置き換える
        Response operator()(const Request &req) {
            const auto &raw_request = req.raw_request();

            PGresult *res;
            int nFields;

            /*
            * この試験ケースではカーソルを使用する。
            *  そのため、トランザクションブロック内で実行する必要がある。
            * 全てを単一の"select * from pg_database"というPQexec()で行うこと
            * も可能だが、例としては簡単過ぎる。
            */

            /* トランザクションブロックを開始する。 */
            res = PQexec(_conn, "BEGIN");
            if (PQresultStatus(res) != PGRES_COMMAND_OK)
            {
                fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(_conn));
                PQclear(res);
                exit_nicely(_conn);
            }

            /*
             * 不要になったら、メモリリークを防ぐためにPGresultをPQclearすべき。
             */
            PQclear(res);

            /*
             * データベースのシステムカタログpg_databaseから行を取り出す。
             */
            res = PQexec(_conn, "DECLARE myportal CURSOR FOR select * from pg_database");
            if (PQresultStatus(res) != PGRES_COMMAND_OK)
            {
                fprintf(stderr, "DECLARE CURSOR failed: %s", PQerrorMessage(_conn));
                PQclear(res);
                exit_nicely(_conn);
            }
            PQclear(res);

            res = PQexec(_conn, "FETCH ALL in myportal");
            if (PQresultStatus(res) != PGRES_TUPLES_OK)
            {
                fprintf(stderr, "FETCH ALL failed: %s", PQerrorMessage(_conn));
                PQclear(res);
                exit_nicely(_conn);
            }

            /* まず属性名を表示する。 */
            nFields = PQnfields(res);
            for (int i = 0; i < nFields; i++)
                printf("%15s", PQfname(res, i));
            printf("\n\n");

            /* そして行を表示する。 */
            for (int i = 0; i < PQntuples(res); i++)
            {
                for (int j = 0; j < nFields; j++)
                    printf("%15s", PQgetvalue(res, i, j));
                printf("\n");
            }

            PQclear(res);

            /* ポータルを閉ざす。ここではエラーチェックは省略した… */
            res = PQexec(_conn, "CLOSE myportal");
            PQclear(res);

            /* トランザクションを終了する */
            res = PQexec(_conn, "END");
            PQclear(res);

            /* データベースとの接続を閉じ、後始末を行う。 */
            PQfinish(_conn);

//            std::vector<std::shared_ptr<CassFuture>> resultFutures;     // CassFuture(実行結果)のsharedptrのベクタを定義
////            resultFutures.reserve(req.queries().size());                // クエリサイズ分予約
//            resultFutures.reserve(req.query().query().size());                // クエリサイズ分予約
////            for (const auto &query : req.queries()) {                   // クエリの数だけ(req.queriesが正しく値が取れていないせつ)
//            debug::hexdump(reinterpret_cast<const char *>(req.query().query().data()), req.query().query().size()); // 下流バッファバッファ16進表示
//            CassStatement* statement = cass_statement_new(req.query().query().data(), 0); // statement, cass_statement_new_n(const char * query, size_t query_length, size_t parameter_count)
//
//            auto result_future = cass_session_execute(_session.get(), statement);
////            }
//            Response res;                                               // response
////            res.reserve(req.queries().size());                          // クエリの数分responseの要素を予約
//            res.reserve(req.query().query().size());                          // クエリの数分responseの要素を予約()
//
//            for (auto &future_: resultFutures) {
//                auto future = future_.get();                            // get():保持しているポインタを取得
//
//                auto errorCode = cass_future_error_code(future);        // 実行結果をエラーコード判定にかける
//                if (errorCode != CASS_OK) {
//                    std::cerr << errorCode << " " << cass_error_desc(errorCode) << std::endl;
//                    /*if (auto errorResult =
//                            CASS_SHARED_PTR(error_result, cass_future_get_error_result(future));
//                        errorResult) {
//                    }*/
//                    res.emplace_back(Status::Error);                    // resの末尾にエラーを表す要素を追加
//                    continue;
//                }

//                auto result = CASS_SHARED_PTR(result, cass_future_get_result(future));      // 成功の結果を取得(futureを待機) TODO ここで待つから遅い
//                if (!result) {
//                    std::cerr << "get result failed" << std::endl;
//                    res.emplace_back(Status::Error);
//                    continue;
//                }

//                auto columnCount = cass_result_column_count(result.get());                  // 指定された結果の 1 行あたりの列数を取得
//                if (columnCount == 0) {
//                    res.emplace_back(Status::Ok);
//                    continue;
//                }
//                auto iterator = CASS_SHARED_PTR(iterator, cass_iterator_from_result(result.get()));  // 指定された結果の新しいイテレータ.結果の行を反復処理するために使用.
//
//                std::vector<std::string> columnNames;
//                for (std::size_t cn = 0; cn < columnCount; ++cn) {
//                    std::size_t length = 0;                                   // 列名の長さ
//                    const char *nbp = nullptr;                                // 列名
//                    cass_result_column_name(result.get(), cn, &nbp, &length); // 指定された結果のインデックスで列名を取得します。(const CassResult * result, size_t index, const char ** name, size_t * name_length)
//                    columnNames.emplace_back(std::string(nbp, nbp + length)); // columnNamesの末尾に列名を追加(文字列の範囲から構築)
//                }

//                std::vector<Row> rows;
//                while (cass_iterator_next(iterator.get())) {                 // イテレータの次の値が存在するまでイテレータを次の行、列、またはコレクション アイテムに進める
//                    Row resRow;
//                    auto row = cass_iterator_get_row(iterator.get());        // resultイテレータの現在の位置にある行を取得。
//                    for (std::size_t i = 0; i < columnNames.size(); ++i) {   // カラム名の数だけ
//                        auto value = cass_row_get_column(row, i);            // 指定された行の index にある列の値を取得 // type->CassValue : 指定されたインデックスの列の値。インデックスが範囲外の場合は NULL が返されます。
//                        resRow[columnNames[i]] = _toRowData(value);          // valueをRowDataに変換
//                    }
//                    rows.emplace_back(resRow);                               // resRowをrowsに末尾に追加
//                }
//
//                res.emplace_back(Status::Result, rows);                      // resの末尾にResult,rowを追加


            return Response({CoResponse(Status::Ok)});;
        }
    };
} // namespace transaction

#undef CASS_SHARED_PTR

