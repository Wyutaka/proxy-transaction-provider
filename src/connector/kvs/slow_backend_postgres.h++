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
        void appned_backend_result_to_response(PGconn* conn, const Request &req, std::queue<response::Sysbench> &results)
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
                response::Sysbench result_record({"pk", "field1", "field2", "field3"}, nFields); // TODO change table by select query
                for (j = 0; j < nFields; j++) {
//                    std::cout << "test1-" << i << "-" << j << std::endl; // ここをコメントアウトするとバグる???
                    result_record.addColumn(PQgetvalue(res, i, j));
                }
                results.emplace(result_record);
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
        // ここをプロキシ(bridge)に置き換える
        Response operator()(const Request &req) {

            if (req.query().isSelect()) {
                std::queue<response::Sysbench> results;
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

