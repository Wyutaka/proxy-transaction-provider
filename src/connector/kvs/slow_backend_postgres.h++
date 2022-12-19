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
        void appned_backend_result_to_response(PGconn* conn, const Request &req, std::queue<std::variant<response::Sysbench, response::Sysbench_one, response::Sysbench_sum>> &results)
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
                exit_nicely(conn);
            }

            /*
             * 不要になったら、メモリリークを防ぐためにPGresultをPQclearすべき。
             *
             */
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
                exit_nicely(conn);
            }
            PQclear(res);

            res = PQexec(conn, "FETCH ALL in myportal");
            if (PQresultStatus(res) != PGRES_TUPLES_OK)
            {
                fprintf(stderr, "FETCH ALL failed: %s", PQerrorMessage(conn));
                PQclear(res);
                exit_nicely(conn);
            }

            nFields = PQnfields(res);

            std::cout << "nFIelds:" << nFields << std::endl;
            std::cout << "PQntuples:" << PQntuples(res) << std::endl;
//            std::cout << "test1-1" << std::endl;
            /* そして行を表示する。 */
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
                } else if (req.query().query()[7] == 's' || req.query().query()[7] == 'S') {
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

    public:
        Response operator()(const Request &req, sqlite3 *in_mem_db) {
            if (req.query().isSelect()) {
                int ret;
                char **err;

//                sqlite3_stmt *statement = nullptr;
//                int prepare_rc = sqlite3_prepare_v2(in_mem_db, "select * from bench;", -1, &statement, nullptr);
//                if (prepare_rc == SQLITE_OK) {
//                    std::cout << "hogehoge" << std::endl;
//                    if (sqlite3_step(statement) == SQLITE_ROW) {
//                        for (int i = 0; i < sqlite3_column_count(statement); i++) {
//                            std::string column_name = sqlite3_column_name(statement, i);
//                            int columnType = sqlite3_column_type(statement, i);
//                            if (columnType == SQLITE_INTEGER) {
//                                std::cout << column_name << " = " << sqlite3_column_int(statement, i) << std::endl;
//                                continue;
//                            }
//                        }
//                    }
//                } else {
//                    printf("ERROR(%d) %s\n", prepare_rc, sqlite3_errmsg(in_mem_db));
//                }
//                sqlite3_reset(statement);
//                sqlite3_finalize(statement);
//                sqlite3_reset(statement);
//                sqlite3_finalize(statement);

//                std::queue<response::Sysbench> results;
                std::queue<std::variant<response::Sysbench, response::Sysbench_one, response::Sysbench_sum>> results;
                appned_backend_result_to_response(_conn, req, results);
                auto res = Response({CoResponse(Status::Result)});
                res.begin()->set_results(results);
                return res;
            }


            return Response({CoResponse(Status::Ok)});;
        }
    };
} // namespace transaction

#undef CASS_SHARED_PTR

