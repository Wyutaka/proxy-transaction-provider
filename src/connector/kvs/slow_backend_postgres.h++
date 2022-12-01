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
        std::queue<std::string> q_queue;

    public:
        explicit PostgresConnector(boost::shared_ptr<tcp_proxy::bridge> bridge, PGconn* conn, std::queue<std::string> &query_queue)
                : _connectFuture(), _conn(conn), _bridge(bridge), q_queue(query_queue) {

        }


        ~PostgresConnector() {
//            _connectFuture.reset();
//            _session.reset();
//            _cluster.reset();
        }

    private:
        void sendQuery(PGconn* conn, const Request &req)
        {

            PGresult *res;
            char *query;
            query = (char*)calloc(15+ req.query().query().size(), sizeof(char));
            sprintf(query, "BEGIN;%s;COMMIT", req.query().query().data());
            PQsendQuery(_conn,  query);
//            if (PQresultStatus(res) != PGRES_COMMAND_OK)
//            {
//                fprintf(stderr, req.query().query().data(), PQerrorMessage(_conn));
//                PQclear(res);
//                exit_nicely(_conn);
//            }
//            PQclear(res);

        }

    public:
        // ここをプロキシ(bridge)に置き換える
        Response operator()(const Request &req) {

            auto th1 = std::thread([this, &req]{sendQuery(_conn, req);});

            th1.join();
            return Response({CoResponse(Status::Ok)});;
        }
    };
} // namespace transaction

#undef CASS_SHARED_PTR

