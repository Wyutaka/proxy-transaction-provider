//
// Created by mint25mt on 2022/08/19.
//

#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/format.hpp>
#include "server.h++"
#include <stdlib.h>
#include <iostream>
#include <libpq-fe.h>
#include <variant>
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
//            exit_nicely(_conn);
        }

        _conn_for_send_query_backend = PQconnectdb(backend_postgres_conninfo);
        /* バックエンドとの接続確立に成功したかを確認する */
        if (PQstatus(_conn_for_send_query_backend) != CONNECTION_OK) {
            fprintf(stderr, "Connection to database failed: %s",
                    PQerrorMessage(_conn_for_send_query_backend));
//            exit_nicely(_conn_for_send_query_backend);
        }

        // インメモリDB用のsqlite3の初期化
        int ret = sqlite3_open(":memory:", &in_mem_db);
        if (ret != SQLITE_OK) {
            std::cout << "FILE OPEN Error";
//            close();
        }

        // sqlite3 データテスト挿入
        ret = sqlite3_exec(in_mem_db, text_create_tbl_bench,
                           NULL, NULL, NULL);
        ret = sqlite3_exec(in_mem_db, text_create_tbl_sbtest1,
                           NULL, NULL, NULL);
        if (ret != SQLITE_OK) {
            printf("ERROR(%d) %s\n", ret, sqlite3_errmsg(in_mem_db));
        }

        ret = sqlite3_exec(in_mem_db, "insert into bench values ('hoge', 1, 2, 3);", NULL, NULL, NULL);
        if (ret != SQLITE_OK) {
            printf("ERROR(%d) %s\n", ret, sqlite3_errmsg(in_mem_db));
        }

        // バックエンドからデータを引き継いで返す
        int nFields;
        int i, j;
        PGresult *res;
        /* トランザクションブロックを開始する。 */
        res = PQexec(_conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(_conn));
            PQclear(res);
//            exit_nicely(_conn);
        }

        PQclear(res);

        /*
         * データベースのシステムカタログpg_databaseから行を取り出す。
         */
        std::string get_cursor_query = "DECLARE myportal CURSOR FOR ";
        res = PQexec(_conn, (get_cursor_query + text_download_sbtest1).c_str()); // TODO クエリの反映
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "DECLARE CURSOR failed: %s", PQerrorMessage(_conn));
            PQclear(res);
//            exit_nicely(_conn);
        }
        PQclear(res);

        res = PQexec(_conn, "FETCH ALL in myportal");
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(stderr, "FETCH ALL failed: %s", PQerrorMessage(_conn));
            PQclear(res);
//            exit_nicely(_conn);
        }

        nFields = PQnfields(res);

        std::cout << "nFIelds:" << nFields << std::endl;
        std::cout << "PQntuples:" << PQntuples(res) << std::endl;
        /* 行を結果に追加。 */
        int row_count = PQntuples(res);
        for (i = 0; i < 3 * row_count / 4 ; i++) { // 50%
            ret = sqlite3_exec(in_mem_db,
                               (boost::format("insert into sbtest1 values ('%1%', %2%, %3%, %4%);") %
                                PQgetvalue(res, i, 0) % PQgetvalue(res, i, 1) % PQgetvalue(res, i, 2) %
                                PQgetvalue(res, i, 3)).str().c_str(),
                               NULL, NULL, NULL);
            if (ret != SQLITE_OK) {
                printf("ERROR(%d) %s\n", ret, sqlite3_errmsg(in_mem_db));
                break;
            }
        }

        std::cout << "sbtest1 cached" << std::endl;
        PQclear(res);

        /* ポータルを閉ざす。ここではエラーチェックは省略した… */
        res = PQexec(_conn, "CLOSE myportal");
        PQclear(res);

        /* トランザクションを終了する */
        res = PQexec(_conn, "END");
        PQclear(res);
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
        } else {
            std::cout << "handle_upstream_connect_err" << std::endl;
            close();
        }
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
        } else {
//            std::cout << "handle_upstream_read_err" << std::endl;
//            std::cout << error.message() << std::endl;
//            close();
        }
    }

    void bridge::handle_downstream_write(const boost::system::error_code &error) {
        if (!error) {
            upstream_socket_.async_read_some(
                    boost::asio::buffer(upstream_data_, max_data_length),
                    boost::bind(&bridge::handle_upstream_read,
                                shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));
        } else {
            std::cout << "handle_downstream_write_err" << std::endl;
            std::cout << error.message() << std::endl;
            close();
        }
    }

    void bridge::handle_downstream_write_proxy(const boost::system::error_code &error) {
        if (!error) {
            downstream_socket_.async_read_some(
                    boost::asio::buffer(upstream_data_, max_data_length),
                    boost::bind(&bridge::handle_downstream_read,
                                shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));
        } else {
            std::cout << "handle_downstream_write_err" << std::endl;
            std::cout << error.message() << std::endl;
            close();
        }
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
//            exit_nicely(conn);
        }
        PQclear(res);
        while (!queue.empty()) {
            std::string_view query = queue.front();
            res = PQexec(conn, query.data());
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                std::cout << "send query backend failed" << std::endl;
                fprintf(stderr, query.data(), PQerrorMessage(conn));
                PQclear(res);
//                exit_nicely(conn);
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
            debug::hexdump(reinterpret_cast<const char *>(downstream_data_), bytes_transferred); // 下流バッファバッファ16進表示 test

            // ヘッダ情報の読み込み
            int n = 0;

            // Cassandraのコード
            if (downstream_data_[0] == 0x04 && downstream_data_[4] == cassprotocol::opcode::QUERY) { //is cassandra request version
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
                // postgresのQueryメッセージ
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

                unsigned char res_postgres[
                        16 + 6] = {0x43, 0x00, 0x00, 0x00, 0x0f, 0x49, 0x4e, 0x53, 0x45, 0x52, 0x54, 0x20, 0x30, 0x20,
                                   0x31, 0x00, // C
                                   0x5a, 0x00, 0x00, 0x00, 0x05, 0x54 // Z
                        };

                if (res.front().status() == transaction::Status::Commit) {
                    queue_sender.submit([&]() { send_queue_backend(query_queue, _conn_for_send_query_backend); });
                }
                std::vector<unsigned char> result;
                // for sysbench
                if (res.front().status() == transaction::Status::Result) {
//                    queue_sender.submit([&]() {send_queue_backend(query_queue, _conn_for_send_query_backend);});
//                    auto D = res.front().get_raw_response();
                    auto Ds = res.front().get_results();
//                    if(req.query().query().data()[])
                    if(req.query().query()[7] == 'c' || req.query().query()[7] == 'D') {
                        for (unsigned char i: response::sysbench_tbl_c_header) { // TODO ベンチマーク毎にヘッダ情報を変えないとだめ
                            result.push_back(i);
                        }
                    } else if(req.query().query()[7] == 's' || req.query().query()[7] == 'S') {
                        for (unsigned char i: response::sysbench_tbl_sum_header) { // TODO ベンチマーク毎にヘッダ情報を変えないとだめ
                            result.push_back(i);
                        }
                    } else {
                        for (unsigned char i: response::sysbench_tbl_header) { // TODO ベンチマーク毎にヘッダ情報を変えないとだめ
                            result.push_back(i);
                        }
                    }
                    while (!Ds.empty()) {
                        if(Ds.front().index() == 0) {
                            std::vector<unsigned char> D = std::get<0>(Ds.front()).bytes();
                            for (unsigned char &i: D) {
                                result.push_back(i);
                            }
                        } else if(Ds.front().index() == 1) {
                            std::vector<unsigned char> D = std::get<1>(Ds.front()).bytes();
                            for (unsigned char &i: D) {
                                result.push_back(i);
                            }
                        } else if(Ds.front().index() == 2) {
                            std::vector<unsigned char> D = std::get<2>(Ds.front()).bytes();
                            for (unsigned char &i: D) {
                                result.push_back(i);
                            }
                        }
                        Ds.pop();
                        if (result.size() > 60000) // TODO resent for rest
                            break;
                    }
                    for (unsigned char i: response::sysbench_slct_cmd) {
                        result.push_back(i);
                    }
                    for (unsigned char i: response::sysbench_status) {
                        result.push_back(i);
                    }
//                    debug::hexdump(reinterpret_cast<const char *>(result.data()), result.size()); // 下流バッファバッファ16進表示

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
                        boost::asio::buffer(downstream_data_,max_data_length),
                        boost::bind(&bridge::handle_downstream_read,
                                    shared_from_this(),
                                    boost::asio::placeholders::error,
                                    boost::asio::placeholders::bytes_transferred));

            // postgresのParseメッセージ(PSの宣言)
            }
            else if (downstream_data_[0] == 0x50){
                // Body部の計算 -> 2,3,4,5バイト目でクエリ全体のサイズ計算 -> クエリの種類と長さの情報を切り取る
                n = (int) downstream_data_[1] << 24;
                n += (int) downstream_data_[2] << 16;
                n += (int) downstream_data_[3] << 8;
                n += (int) downstream_data_[4];

//            debug::hexdump(reinterpret_cast<const char *>(downstream_data_), bytes_transferred); // 下流バッファバッファ16進表示
                int prepared_statement_id_length = 0;
                while(downstream_data_[prepared_statement_id_length + 5] != '\0') { // 5はメッセージ形式とメッセージ帳のバイト長だけ進めることを意味する
                    prepared_statement_id_length++;
                }
                auto prepared_statement_id = std::string(reinterpret_cast<const char *>(&downstream_data_[5]), prepared_statement_id_length);
                auto prepared_statement_query = std::string(reinterpret_cast<const char *>(&downstream_data_[5 + prepared_statement_id_length]), n - 4 - prepared_statement_id_length);

                prepared_statements_lists.insert(std::make_pair(prepared_statement_id, prepared_statement_query));

                for (auto & prepared_statements_list : prepared_statements_lists) {
                    std::cout << "itr test :: first_id: " << prepared_statements_list.first << " second_query: " << prepared_statements_list.second << std::endl;
                }
                async_write(upstream_socket_,
                            boost::asio::buffer(downstream_data_, bytes_transferred),
                            boost::bind(&bridge::handle_upstream_write,
                                        shared_from_this(),
                                        boost::asio::placeholders::error));
            }
            else {
                async_write(upstream_socket_,
                            boost::asio::buffer(downstream_data_, bytes_transferred),
                            boost::bind(&bridge::handle_upstream_write,
                                        shared_from_this(),
                                        boost::asio::placeholders::error));
            }

        } else { // if (!error)
            std::cout << "handle_downstream_read_err" << std::endl;
            std::cout << error.message() << std::endl;
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
        } else {
            std::cout << "handle_upstream_write_err" << std::endl;
            close();
        }
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
