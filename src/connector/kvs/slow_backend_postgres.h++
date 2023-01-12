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
#include <variant>
#include <libpq-fe.h>
#include "./src/lock/Lock.h++"
#include "./src/transaction/transaction_impl.hpp"
#include "./src/test/DumpHex.h++"

static void
exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

const bool CHACHED = true;

namespace transaction {

    class PostgresConnector {
    private:
        boost::shared_ptr<tcp_proxy::bridge> _bridge;
        std::shared_ptr<CassFuture> _connectFuture;
        PGconn* _conn;

    public:
        explicit PostgresConnector(boost::shared_ptr<tcp_proxy::bridge> bridge, PGconn* conn)
                : _connectFuture(), _conn(conn), _bridge(bridge) {
        }

    private:
        static void download_result(PGconn* conn, const Request &req, std::queue<response::sysbench_result_type> &results)
        {

            int                     nFields;
            int                     i,j;
            PGresult *res;
            /* トランザクションブロックを開始する。 */
            res = PQexec(conn, "BEGIN");
            if (PQresultStatus(res) != PGRES_COMMAND_OK)
            {
                fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
                PQclear(res);
//                exit_nicely(conn);
            }

            PQclear(res);

            /*
             * データベースのシステムカタログpg_databaseから行を取り出す。
             */
            auto get_cursor_query = "DECLARE myportal CURSOR FOR ";
            std::cout << std::string(req.query().query().data()).c_str() << std::endl;
            res = PQexec(conn, (get_cursor_query + std::string(req.query().query().data())).c_str()); // TODO クエリの反映
            if (PQresultStatus(res) != PGRES_COMMAND_OK)
            {
                fprintf(stderr, "DECLARE CURSOR failed: %s", PQerrorMessage(conn));
                PQclear(res);
//                exit_nicely(conn);
            }
            PQclear(res);

            res = PQexec(conn, "FETCH ALL in myportal");
            if (PQresultStatus(res) != PGRES_TUPLES_OK)
            {
                fprintf(stderr, "FETCH ALL failed: %s", PQerrorMessage(conn));
                PQclear(res);
//                exit_nicely(conn);
            }

            nFields = PQnfields(res);

            std::cout << "nFIelds:" << nFields << std::endl;
            std::cout << "PQntuples:" << PQntuples(res) << std::endl;
//            std::cout << "test1-1" << std::endl;
            /* 行を結果に追加。 */
            for (i = 0; i < PQntuples(res); i++)
            {
                if (nFields == 4) { // resultが4つの時
                    response::Sysbench result_record({"id", "k", "c", "pad"}, nFields); // TODO change table by select query
                    for (j = 0; j < nFields; j++) {
                        result_record.addColumn(PQgetvalue(res, i, j));
                    }
                    results.emplace(result_record);
                } else if (req.query().query()[7] == 'c' || req.query().query()[7] == 'D') { // DISTINCT or c
                    response::Sysbench_one result_record({"c"}, nFields); // TODO change table by select query
                    for (j = 0; j < nFields; j++) {
                        result_record.addColumn(PQgetvalue(res, i, j));
                    }
                    results.emplace(result_record);
                } else if (req.query().query()[7] == 's' || req.query().query()[7] == 'S') { // select sum
                    response::Sysbench_one result_record({"sum"}, nFields); // TODO change table by select query
                    for (j = 0; j < nFields; j++) {
                        result_record.addColumn(PQgetvalue(res, i, j));
                    }
                    results.emplace(result_record);
                }
            }

            PQclear(res);

            /* ポータルを閉ざす。ここではエラーチェックは省略した… */
            res = PQexec(conn, "CLOSE myportal");
            PQclear(res);

            /* トランザクションを終了する */
            res = PQexec(conn, "END");
            PQclear(res);
        }

        /*
         * インメモリdbから値を取得する.インメモリデータがある場合はtrueを返す.
         * sysbenchを回すためにハードコードしてるぶぶんがあるので、直す
         */
        bool get_from_local(sqlite3 *in_mem_db, const Request &req, std::queue<response::sysbench_result_type> &results) {
            if (req.query().isSelect()) {
                int row_count;
                sqlite3_stmt *statement = nullptr;
                int prepare_rc = sqlite3_prepare_v2(in_mem_db, req.query().query().data(), -1, &statement, nullptr);
                if (prepare_rc == SQLITE_OK) {
                    while (sqlite3_step(statement) == SQLITE_ROW) {
                        row_count++;
                        int columnCount = sqlite3_column_count(statement);
                        if (columnCount == 4) { // resultが4つの時(select * の時)
                            response::Sysbench result_record({"id", "k", "c", "pad"}, columnCount); // TODO change table by select query
                            for (int j = 0; j < columnCount; j++) {
                                int columnType = sqlite3_column_type(statement, j);
                                switch (columnType) {
                                    case SQLITE_TEXT:
                                        result_record.addColumn(sqlite3_column_text(statement, j));
                                        break;
                                    case SQLITE_INTEGER:
                                        result_record.addColumn(sqlite3_column_int(statement, j));
                                        break;
                                }
                            }
                            results.emplace(result_record);
                        } else if (req.query().query()[7] == 'c' || req.query().query()[7] == 'D') { // DISTINCT or c
                            response::Sysbench_one result_record({"c"}, columnCount); // TODO change table by select query
                            for (int j = 0; j < columnCount; j++) {
                                int columnType = sqlite3_column_type(statement, j);
                                switch (columnType) {
                                    case SQLITE_TEXT:
                                        result_record.addColumn(sqlite3_column_text(statement, j));
                                        break;
                                    case SQLITE_INTEGER:
                                        result_record.addColumn(sqlite3_column_int(statement, j));
                                        break;
                                }
                            }
                            results.emplace(result_record);
                        } else if (req.query().query()[7] == 's' || req.query().query()[7] == 'S') { // select sum
                            response::Sysbench_one result_record({"sum"}, columnCount); // TODO change table by select query
                            for (int j = 0; j < columnCount; j++) {
                                int columnType = sqlite3_column_type(statement, j);
                                switch (columnType) {
                                    case SQLITE_TEXT:
                                        result_record.addColumn(sqlite3_column_text(statement, j));
                                        break;
                                    case SQLITE_INTEGER:
                                        result_record.addColumn(sqlite3_column_int(statement, j));
                                        break;
                                }
                            }
                            results.emplace(result_record);
                        }
                    }
                } else {
                    printf("ERROR(%d) %s\n", prepare_rc, sqlite3_errmsg(in_mem_db));
                }
                sqlite3_reset(statement);
                sqlite3_finalize(statement);

                return row_count > 0;
            }
        }

    public:
        Response operator()(const Request &req, sqlite3 *in_mem_db) {
//            debug::hexdump(req.query().query().data(), req.query().query().size()); // for test
            if (req.query().isSelect()) {
                std::queue<response::sysbench_result_type> results;
                bool isCached = get_from_local(in_mem_db, req, results);
                if (!isCached) {
//                    std::cout << "not cached" << std::endl;
                    download_result(_conn, req, results);
                }

                auto res = Response({CoResponse(Status::Result)});
                res.begin()->set_results(results);
                return res;
            }


            return Response({CoResponse(Status::Ok)});;
        }
    };
} // namespace transaction

#undef CASS_SHARED_PTR

