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

    class PostgresConnector {
    private:
        boost::shared_ptr<tcp_proxy::bridge> _bridge;
        std::shared_ptr<CassFuture> _connectFuture;
        PGconn* _conn;

    public:
        explicit PostgresConnector(boost::shared_ptr<tcp_proxy::bridge> bridge, PGconn* conn)
                : _connectFuture(), _conn(conn), _bridge(bridge) {

        }


        ~PostgresConnector() {
//            _connectFuture.reset();
//            _session.reset();
//            _cluster.reset();
        }

    private:

    public:
        // ここをプロキシ(bridge)に置き換える
        Response operator()(const Request &req) {

            PGresult *res;

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
            res = PQexec(_conn, req.query().query().data());
            if (PQresultStatus(res) != PGRES_COMMAND_OK)
            {
                fprintf(stderr, req.query().query().data(), PQerrorMessage(_conn));
                PQclear(res);
                exit_nicely(_conn);
            }
            PQclear(res);

            /* トランザクションを終了する */
            res = PQexec(_conn, "END");
            PQclear(res);

//            /* データベースとの接続を閉じ、後始末を行う。 */
//            PQfinish(_conn);

            return Response({CoResponse(Status::Ok)});;
        }
    };
} // namespace transaction

#undef CASS_SHARED_PTR

