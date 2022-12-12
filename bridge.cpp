//
// Created by mint25mt on 2022/08/19.
//

#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include "server.h++"
#include <stdlib.h>
#include <iostream>
#include <libpq-fe.h>
#include "./src/lock/Lock.h++"
#include "./src/transaction/transaction_impl.hpp"
#include "./src/test/DumpHex.h++"
#include "./src/connector/kvs/cassprotocol.h++"
#include "./src/reqestresponse/Constants.h++"
#include "src/connector/kvs/slow_postgres.h++"
#include "src/ThreadPool/ThreadPool.h++"

#define CASS_SHARED_PTR(type, v)                                                                   \
    std::shared_ptr<std::remove_pointer_t<decltype(v)>>(v, [](decltype(v) t) {                     \
        if (t)                                                                                     \
            cass_##type##_free(t);                                                                 \
    })

namespace tcp_proxy {
    typedef ip::tcp::socket socket_type;
    typedef boost::shared_ptr<bridge> ptr_type;

    bridge::bridge(boost::asio::io_service &ios, unsigned short upstream_port, std::string upstream_host)
            : downstream_socket_(ios),
              upstream_socket_(ios),
              upstream_host_(upstream_host_),
              upstream_port_(upstream_port_),
              _connectFuture(NULL),
              _cluster(CASS_SHARED_PTR(cluster, cass_cluster_new())),
              _session(std::shared_ptr<CassSession>(cass_session_new(), transaction::detail::SessionDeleter())),
              queue_sender(pool::ThreadPoolExecutor(1)) {
        // cassandraのコネクション
        // 注意：keyspaceは決め打ち
        cass_cluster_set_contact_points(_cluster.get(), backend_host);
        cass_cluster_set_protocol_version(_cluster.get(), CASS_PROTOCOL_VERSION_V4);
        _connectFuture =
                CASS_SHARED_PTR(future, cass_session_connect_keyspace_n(_session.get(), _cluster.get(), "txbench",
                                                                        7)); //7-> keyspace_length;

        if (cass_future_error_code(_connectFuture.get()) != CASS_OK) {
            std::cerr << "cannot connect to Cassandra" << std::endl;
            std::terminate();
        }

//                // postgresのコネクションを作成
        _conn = PQconnectdb(backend_postgres_conninfo);
        /* バックエンドとの接続確立に成功したかを確認する */
        if (PQstatus(_conn) != CONNECTION_OK) {
            fprintf(stderr, "Connection to database failed: %s",
                    PQerrorMessage(_conn));
            exit_nicely(_conn);
        }

        _conn_for_send_query_backend = PQconnectdb(backend_postgres_conninfo);
        /* バックエンドとの接続確立に成功したかを確認する */
        if (PQstatus(_conn_for_send_query_backend) != CONNECTION_OK) {
            fprintf(stderr, "Connection to database failed: %s",
                    PQerrorMessage(_conn_for_send_query_backend));
            exit_nicely(_conn_for_send_query_backend);
        }

        // インメモリDB用のsqlite3の初期化
        int ret = sqlite3_open("memory:", &in_mem_db);
        if (ret != SQLITE_OK) {
            std::cout << "FILE OPEN Error";
            close();
        }

        // sqlite3 データテスト挿入
        ret = sqlite3_exec(in_mem_db,
                           "create table bench (pk text primary key, field1 integer, field2 integer, field3 integer)",
                           NULL, NULL, NULL);
        if (ret != SQLITE_OK) {
            printf("ERROR(%d) %s\n", ret, sqlite3_errmsg(in_mem_db));
        }
        ret = sqlite3_exec(in_mem_db, "insert into bench values ('hoge', 1, 2, 3);", NULL, NULL, NULL);
        if (ret != SQLITE_OK) {
            printf("ERROR(%d) %s\n", ret, sqlite3_errmsg(in_mem_db));
        }
    }

    bridge::~bridge() {
        _connectFuture.reset();
        _session.reset();
        _cluster.reset();
        sqlite3_close(in_mem_db);
        PQfinish(_conn);
    }

    socket_type &bridge::downstream_socket() {
        // Client socket
        return downstream_socket_;
    }

    socket_type &bridge::upstream_socket() {
        // Remote server socket
        return upstream_socket_;
    }

    void bridge::start(const std::string &upstream_host, unsigned short upstream_port) {
        // Attempt connection to remote server (upstream side)
        upstream_socket_.async_connect(
                ip::tcp::endpoint(
                        boost::asio::ip::address::from_string(upstream_host),
                        upstream_port),
                boost::bind(&bridge::handle_upstream_connect,
                            shared_from_this(),
                            boost::asio::placeholders::error));
    }

    void bridge::handle_upstream_connect(const boost::system::error_code &error) {
        if (!error) {
            // Setup async read from remote server (upstream)
            upstream_socket_.async_read_some(
                    boost::asio::buffer(upstream_data_, max_data_length),
                    boost::bind(&bridge::handle_upstream_read,
                                shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));

            // Setup async read from client (downstream)
            downstream_socket_.async_read_some(
                    boost::asio::buffer(downstream_data_, max_data_length),
                    boost::bind(&bridge::handle_downstream_read,
                                shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));
        } else
            close();
    }

    void bridge::handle_upstream_read(const boost::system::error_code &error,
                                      const size_t &bytes_transferred) {
        if (!error) {

//            std::cout << "handle upstream_read" << std::endl;
//            debug::hexdump(reinterpret_cast<const char *>(upstream_data_), bytes_transferred);

            async_write(downstream_socket_,
                        boost::asio::buffer(upstream_data_, bytes_transferred),
                        boost::bind(&bridge::handle_downstream_write,
                                    shared_from_this(),
                                    boost::asio::placeholders::error));
        } else
            close();
    }

    void bridge::handle_downstream_write(const boost::system::error_code &error) {
        if (!error) {
            upstream_socket_.async_read_some(
                    boost::asio::buffer(upstream_data_, max_data_length),
                    boost::bind(&bridge::handle_upstream_read,
                                shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));
        } else
            close();
    }

    template<class C>
    void print(const C &c, std::ostream &os = std::cout) {
        std::for_each(std::begin(c), std::end(c),
                      [&os](typename C::value_type p) { os << '{' << p.first << ',' << &p.second << "}, "; });
        os << std::endl;
    }

    void bridge::send_queue_backend(std::queue<std::string> &queue, PGconn *conn) {
        PGresult *res;
        res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
            PQclear(res);
            exit_nicely(conn);
        }
        PQclear(res);
        while (!queue.empty()) {
            std::string_view query = queue.front();
            res = PQexec(conn, query.data());
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                fprintf(stderr, query.data(), PQerrorMessage(conn));
                PQclear(res);
                exit_nicely(conn);
            }
            queue.pop();
            PQclear(res);
        }

        /* トランザクションを終了する */
        res = PQexec(conn, "END");
        PQclear(res);
    }

    void bridge::handle_downstream_read(const boost::system::error_code &error,
                                        const size_t &bytes_transferred) {
        if (!error) {
//            std::cout << "handle downstream_read" << std::endl;
//            debug::hexdump(reinterpret_cast<const char *>(downstream_data_), bytes_transferred); // 下流バッファバッファ16進表示

            // ヘッダ情報の読み込み
            int n = 0;

            // Cassandraのコード
            if (downstream_data_[0] == 0x04 &&
                downstream_data_[4] == cassprotocol::opcode::QUERY) { //is cassandra request version
//                std::cout << "cql detected" << std::endl;
                n = (int) downstream_data_[5] << 24;
                n += (int) downstream_data_[6] << 16;
                n += (int) downstream_data_[7] << 8;
                n += (int) downstream_data_[8];
//                std::cout << "one query size" << n << std::endl;
                if (n != 0) {

                    transaction::lock::Lock<transaction::SlowCassandraConnector> lock{
                            transaction::SlowCassandraConnector(
                                    transaction::SlowCassandraConnector(backend_host, shared_from_this()))};
                    // 13~bodyまで切り取り
                    const transaction::Request &req = transaction::Request(
                            transaction::Peer(upstream_host_, upstream_port_),
                            std::string(reinterpret_cast<const char *>(&downstream_data_[13]), n),
                            std::string(reinterpret_cast<const char *>(&downstream_data_), bytes_transferred));

                    // レイヤー移動
                    const auto &res = lock(req, write_ahead_log, query_queue, in_mem_db);

                    if (res.begin()->status() == transaction::Status::Ok) {// レスポンスでOKが帰って来たとき(begin,commitを想定)
                        std::cout << "status ok" << std::endl;
                        // クエリに対するリクエストを返す
                        // ヘッダ + レスポンスボディ
                        // 原型生成
                        unsigned char result_ok[13] = {0x84, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00,
                                                       0x00, 0x01};
//                        memcpy(&result_ok[2], &downstream_data_[2], 2);
                        result_ok[2] = downstream_data_[2];         // streamIDの置換
                        result_ok[3] = downstream_data_[3];         // streamIDの置換
//                        debug::hexdump(result_ok, 4);
//                        std::cout << result_ok[0] << std::endl;
                        // streamIdコピー
                        async_write(downstream_socket_,
                                    boost::asio::buffer(result_ok, 13), // result_okの文字列長
                                    boost::bind(&bridge::handle_downstream_write,
                                                shared_from_this(),
                                                boost::asio::placeholders::error));

                        // Setup async read from client (downstream)
                        downstream_socket_.async_read_some(
                                boost::asio::buffer(downstream_data_, max_data_length),
                                boost::bind(&bridge::handle_downstream_read,
                                            shared_from_this(),
                                            boost::asio::placeholders::error,
                                            boost::asio::placeholders::bytes_transferred));
                    }
                }
                // postgresのコード
            } else if (downstream_data_[0] == 0x51) { // 1バイト目が'Q'のとき

//                std::cout << "postgres" << std::endl;

                // Body部の計算 -> 2,3,4,5バイト目でクエリ全体のサイズ計算 -> クエリの種類と長さの情報を切り取る
                n = (int) downstream_data_[1] << 24;
                n += (int) downstream_data_[2] << 16;
                n += (int) downstream_data_[3] << 8;
                n += (int) downstream_data_[4];

                // lock層の生成(postgres用)
                transaction::lock::Lock<transaction::PostgresConnector> lock{
                        transaction::PostgresConnector(transaction::PostgresConnector(shared_from_this(), _conn))};
                // リクエストの生成
                const transaction::Request &req = transaction::Request(
                        transaction::Peer(upstream_host_, upstream_port_),
                        std::string(reinterpret_cast<const char *>(&downstream_data_[5]), n - 4),
                        std::string(reinterpret_cast<const char *>(&downstream_data_),
                                    bytes_transferred)); // n-4 00まで含める｀h
                // レスポンス生成
                const auto &res = lock(req, write_ahead_log, query_queue, in_mem_db);

                // if (res[0].status() == transaction::Status::Ok) {
                //     std::cout << "Status OK" << std::endl;
                // }

                /* TODO クエリに対するリクエストを返す
                     クライアントに返す文字列の形式: <C/Z
                     Byte1('C') |  Int32       |  String    |  Byte1('Z')  |  Int32(5)   |  Byte1
                     C          |   Int32      |  hoge      |      Z       | 00 00 00 05 |  {'I'|'T'|'E'}  ('I'-> not in transaction/'T'-> in transaction/'E'-> 'Error transaction')
                     43         |  xx xx xx xx | xx xx ...  |      5a      | 00 00 00 05 |  {49/54/45}
                */

                /* TODO クエリに対するリクエストを返す(select)
                    クライアントに返す文字列の形式: <T(row description)/D(Data row)/C(Command Completion)/Z(Ready for Query)
                    bench("huga", 1, 2, 3)
                    T: length: 103 {0x54, 0x00, 0x00, 0x00, 0x66, 0x00, 0x04, 0x70, 0x6b, 0x00, 0x00, 0x00, 0x42, 0x00,
                    0x00, 0x01,0x00, 0x00, 0x04, 0x13, 0xff, 0xff, 0x00, 0x00, 0x00, 0x25, 0x00, 0x00, 0x66, 0x69,
                    0x65, 0x6c, 0x64, 0x31, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x17, 0x00,
                    0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x32, 0x00, 0x00, 0x00,
                    0x42, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x17, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00,
                    0x66, 0x69, 0x65, 0x6c, 0x64, 0x33, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
                    0x17, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00}
                    D: length: 30 {0x44, 0x00, 0x00, 0x00, 0x1d, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04, 0x68, 0x75, 0x67, 0x61,
                            0x00, 0x00, 0x00, 0x01, 0x31, 0x00, 0x00, 0x00, 0x01, 0x32, 0x00, 0x00, 0x00, 0x01, 0x33}
                    C: length: 14 {0x43, 0x00, 0x00, 0x00, 0x0d, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x31, 0x00}
                    Z: length: 6 {0x5a, 0x00, 0x00, 0x00, 0x05, 0x49}
                 */


                unsigned char res_postgres[
                        16 + 6] = {0x43, 0x00, 0x00, 0x00, 0x0f, 0x49, 0x4e, 0x53, 0x45, 0x52, 0x54, 0x20, 0x30, 0x20,
                                   0x31, 0x00, // C
                                   0x5a, 0x00, 0x00, 0x00, 0x05, 0x54 // Z
                        };
                // unsigned char res_postgres[154] = {0x54, 0x00, 0x00, 0x00, 0x66, 0x00, 0x04, 0x70, 0x6b, 0x00, 0x00, 0x00, 0x42, 0x00,
                //                                    0x00, 0x01,0x00, 0x00, 0x04, 0x13, 0xff, 0xff, 0x00, 0x00, 0x00, 0x25, 0x00, 0x00, 0x66, 0x69,
                //                                    0x65, 0x6c, 0x64, 0x31, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x17, 0x00,
                //                                    0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x32, 0x00, 0x00, 0x00,
                //                                    0x42, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x17, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00,
                //                                    0x66, 0x69, 0x65, 0x6c, 0x64, 0x33, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
                //                                    0x17, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x44, 0x00, 0x00, 0x00, 0x1d, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04, 0x68, 0x75, 0x67, 0x61,
                //                                    0x00, 0x00, 0x00, 0x01, 0x31, 0x00, 0x00, 0x00, 0x01, 0x32, 0x00, 0x00, 0x00, 0x01, 0x33,
                //                                    0x43, 0x00, 0x00, 0x00, 0x0d, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x31, 0x00,
                //                                    0x5a, 0x00, 0x00, 0x00, 0x05, 0x49};

//                unsigned char *ptr = &(res_postgres[0]);
                if (res.front().status() == transaction::Status::Commit) {
                    queue_sender.submit([&]() { send_queue_backend(query_queue, _conn_for_send_query_backend); });
                }


//                if (res.front().status() == transaction::Status::Error) {
//                    std::cout << "status ERROR" << std::endl;
//                    res_postgres[21] = 0x45;
//                }


                // for (auto itr = res.begin(); itr < res.end(); itr++) {
                //     std::cout << (int)itr->status() << std::endl;
                //     switch (itr->status()) {
                //         case transaction::Status::Result:
                //             std::cout << "result" << std::endl;
                //             break;
                //         case transaction::Status::Pending:
                //             std::cout << "pending" << std::endl;
                //             break;

                //     }
                // }

                // Dの追加
                // if(res.end()->status() == transaction::Status::Result) {
                //     std::cout << "result is arimasu" << std::endl;
                //     std::vector<unsigned char> data = res.end()->get_raw_response(); // この辺がおかしい
                //     debug::hexdump(reinterpret_cast<const char *>(data.data()), data.size());
                // }

                std::vector<unsigned char> result;
                if (res.front().status() == transaction::Status::Result) {
//                    queue_sender.submit([&]() {send_queue_backend(query_queue, _conn_for_send_query_backend);});
//                    auto D = res.front().get_raw_response();
                    auto Ds = res.front().get_results();
                    for (unsigned char i: response::sysbench_tbl_header) {
                        result.push_back(i);
                    }
                    while (!Ds.empty()) {
                        std::vector<unsigned char> D = Ds.front().bytes();
                        for (unsigned char &i: D) {
                            result.push_back(i);
                        }
                        Ds.pop();
                    }
                    for (unsigned char i: response::sysbench_slct_cmd) {
                        result.push_back(i);
                    }
                    for (unsigned char i: response::sysbench_status) {
                        result.push_back(i);
                    }

                }

                if (res.front().status() == transaction::Status::Result) {
                    async_write(downstream_socket_,
                                boost::asio::buffer(result.data(), result.size()), // result_okの文字列長
                                boost::bind(&bridge::handle_downstream_write,
                                            shared_from_this(),
                                            boost::asio::placeholders::error));
                } else {
                    // TODO 失敗、成功をレスポンスから判定して返す
                    // レスポンス帰る前に送ってる？？
                    // クライアントにレスポンスを返す
                    async_write(downstream_socket_,
                                boost::asio::buffer(res_postgres, 154), // result_okの文字列長
                                boost::bind(&bridge::handle_downstream_write,
                                            shared_from_this(),
                                            boost::asio::placeholders::error));
                }

                // クライアントからの読み込みを開始 (downstream)
                downstream_socket_.async_read_some(
                        boost::asio::buffer(downstream_data_, max_data_length),
                        boost::bind(&bridge::handle_downstream_read,
                                    shared_from_this(),
                                    boost::asio::placeholders::error,
                                    boost::asio::placeholders::bytes_transferred));



//                async_write(upstream_socket_,
//                            boost::asio::buffer(downstream_data_,bytes_transferred),
//                            boost::bind(&bridge::handle_upstream_write,
//                                        shared_from_this(),
//                                        boost::asio::placeholders::error));

            } else {
                async_write(upstream_socket_,
                            boost::asio::buffer(downstream_data_, bytes_transferred),
                            boost::bind(&bridge::handle_upstream_write,
                                        shared_from_this(),
                                        boost::asio::placeholders::error));
            }

        } else {
            close();
        }
    }


    void bridge::handle_upstream_write(const boost::system::error_code &error) {
        if (!error) {
            downstream_socket_.async_read_some(
                    boost::asio::buffer(downstream_data_, max_data_length),
                    boost::bind(&bridge::handle_downstream_read,
                                shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));
        } else
            close();
    }

    void bridge::close() {
        boost::mutex::scoped_lock lock(mutex_);

        if (downstream_socket_.is_open()) {
            downstream_socket_.close();
        }

        if (upstream_socket_.is_open()) {
            upstream_socket_.close();
        }
    }

}// namespace tcp_proxy
