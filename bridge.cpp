//
// Created by mint25mt on 2022/08/19.
//

#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/format.hpp>
#include <boost/stacktrace.hpp>
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
              upstream_host_(upstream_host),
              upstream_port_(upstream_port),
              _connectFuture(NULL),
              _cluster(CASS_SHARED_PTR(cluster, cass_cluster_new())),
              _session(std::shared_ptr<CassSession>(cass_session_new(), transaction::detail::SessionDeleter())),
              queue_sender(pool::ThreadPoolExecutor(1)) {

//        // cassandraのコネクション
//        // 注意：keyspaceは決め打ち
//        cass_cluster_set_contact_points(_cluster.get(), backend_host);
//        cass_cluster_set_protocol_version(_cluster.get(), CASS_PROTOCOL_VERSION_V4);
//        _connectFuture =
//                CASS_SHARED_PTR(future, cass_session_connect_keyspace_n(_session.get(), _cluster.get(), "txbench",
//                                                                        7)); //7-> keyspace_length;
//
//        if (cass_future_error_code(_connectFuture.get()) != CASS_OK) {
//            std::cerr << "cannot connect to Cassandra" << std::endl;
//            std::terminate();
//        }

        // postgresのコネクション
        _conn = PQconnectdb(backend_postgres_conninfo);
        /* バックエンドとの接続確立に成功したかを確認する */
        if (PQstatus(_conn) != CONNECTION_OK) {
            fprintf(stderr, "Connection to database failed: %s",
                    PQerrorMessage(_conn));
            exit_nicely(_conn);
        }

//        bridge::connectToPostgres(_conn, backend_postgres_conninfo);
        bridge::initializeSQLite(_conn, text_create_tbl_sbtest1);
//        bridge::fetchAndCacheData(_conn, in_mem_db, text_download_sbtest1);
    }

    // PostgreSQLデータベースへの接続の設定
    void bridge::connectToPostgres(PGconn *_conn, const char *backend_postgres_connInfo) {
        _conn = PQconnectdb(backend_postgres_connInfo);
        if (PQstatus(_conn) != CONNECTION_OK) {
            fprintf(stderr, "Connection to database failed: %s",
                    PQerrorMessage(_conn));
            exit_nicely(_conn);
        }
    }

    // SQLiteのインメモリデータベースの初期化
    void bridge::initializeSQLite(PGconn *_conn, const char *text_create_tbl) {
        sqlite3 *in_mem_db;
        int ret = sqlite3_open(":memory:", &in_mem_db);
        if (ret != SQLITE_OK) {
            std::cout << "FILE OPEN Error";
            close_and_reset();
        }

        ret = sqlite3_exec(in_mem_db, text_create_tbl,
                           NULL, NULL, NULL);
        if (ret != SQLITE_OK) {
            printf("ERROR(%d) %s\n", ret, sqlite3_errmsg(in_mem_db));
        }
    }

    // PostgreSQLデータベースからデータを取得して、それをSQLiteデータベースに保存する
    void bridge::fetchAndCacheData(PGconn *_conn, sqlite3 *in_mem_db, const char *text_download_tbl) {
        int nFields;
        PGresult *res;
        /* トランザクションブロックを開始する。 */
        res = PQexec(_conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(_conn));
            PQclear(res);
            exit_nicely(_conn);
        }

        PQclear(res);

        /*
         * データベースのシステムカタログpg_databaseから行を取り出す。
         */
        std::string get_cursor_query = "DECLARE myportal CURSOR FOR ";
        res = PQexec(_conn, (get_cursor_query + text_download_tbl).c_str()); // TODO クエリの反映
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "DECLARE CURSOR failed: %s", PQerrorMessage(_conn));
            PQclear(res);
//            exit_nicely(_conn);
        }
        PQclear(res);

        res = PQexec(_conn, "FETCH ALL in myportal");
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(stderr, "FETCH ALL failed: %s", PQerrorMessage(_conn));
//            PQclear(res);
//            exit_nicely(_conn);
        }

        nFields = PQnfields(res);

        std::cout << "nFIelds:" << nFields << std::endl;
        std::cout << "PQntuples:" << PQntuples(res) << std::endl;
        /* 行を結果に追加。 */
        int row_count = PQntuples(res);
//        for (i = 0; i < row_count; i++) { // 50%
//            ret = sqlite3_exec(in_mem_db,
//                               (boost::format("insert into sbtest1 values ('%1%', %2%, %3%, %4%);") %
//                                PQgetvalue(res, i, 0) % PQgetvalue(res, i, 1) % PQgetvalue(res, i, 2) %
//                                PQgetvalue(res, i, 3)).str().c_str(),
//                               NULL, NULL, NULL);
//            if (ret != SQLITE_OK) {
//                printf("ERROR(%d) %s\n", ret, sqlite3_errmsg(in_mem_db));
//                break;
//            }
//        }

        std::cout << "sbtest1 cached" << std::endl;
        PQclear(res);

        /* ポータルを閉ざす。ここではエラーチェックは省略した… */
        res = PQexec(_conn, "CLOSE myportal");
        PQclear(res);

        /* トランザクションを終了する */
        res = PQexec(_conn, "END");
        PQclear(res);

        // 省略: FETCH, データの処理と挿入, トランザクションの終了など
    }

    void download_result(PGconn &conn, const transaction::Request &req, sqlite3 *in_mem_db) {
        int ret = sqlite3_open(":memory:", &in_mem_db);
        if (ret != SQLITE_OK) {
            std::cout << "FILE OPEN Error";
//            close_and_reset();
        }
        int nFields;
        int i, j;
        PGresult *res;
        /* トランザクションブロックを開始する。 */
        res = PQexec(&conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(&conn));
            PQclear(res);
//            exit_nicely(_conn);
        }

        PQclear(res);

        /*
         * データベースのシステムカタログpg_databaseから行を取り出す。
         */
        std::string get_cursor_query = "DECLARE myportal CURSOR FOR ";
        res = PQexec(&conn, (get_cursor_query + req.query().query().data()).c_str()); // TODO クエリの反映
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "DECLARE CURSOR failed: %s", PQerrorMessage(&conn));
            PQclear(res);
        }
        PQclear(res);

        res = PQexec(&conn, "FETCH ALL in myportal");
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(stderr, "FETCH ALL failed: %s", PQerrorMessage(&conn));
            PQclear(res);
        }

        nFields = PQnfields(res);

        std::cout << "nFIelds:" << nFields << std::endl;
        std::cout << "PQntuples:" << PQntuples(res) << std::endl;
        /* 行を結果に追加。 */
        int row_count = PQntuples(res);
        for (i = 0; i < row_count; i++) {
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
        res = PQexec(&conn, "CLOSE myportal");
        PQclear(res);

        /* トランザクションを終了する */
        res = PQexec(&conn, "END");
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
        std::cout << "upstream_host " << upstream_host << std::endl;
        std::cout << "upstream_port " << upstream_port << std::endl;

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
            // error の内容を表示
            std::cout << "Error code: " << error.value() << std::endl;
            std::cout << "Error message: " << error.message() << std::endl;
            close_and_reset();
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
//            close_and_reset();
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
            close_and_reset();
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
            close_and_reset();
        }
    }

    template<class C>
    void print(const C &c, std::ostream &os = std::cout) {
        std::for_each(std::begin(c), std::end(c),
                      [&os](typename C::value_type p) { os << '{' << p.first << ',' << &p.second << "}, "; });
        os << std::endl;
    }

    void bridge::send_queue_backend(std::queue<std::string> &queue) {
        thread_local PGconn *_conn_for_send_query_backend = PQconnectdb(backend_postgres_conninfo);
        /* バックエンドとの接続確立に成功したかを確認する */
        if (PQstatus(_conn_for_send_query_backend) != CONNECTION_OK) {
            fprintf(stderr, "Connection to database failed: %s",
                    PQerrorMessage(_conn_for_send_query_backend));
//            exit_nicely(_conn_for_send_query_backend);
        }

        PGresult *res;
        res = PQexec(_conn_for_send_query_backend, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(_conn_for_send_query_backend));
            exit_nicely(_conn_for_send_query_backend);
        }
        PQclear(res);
        while (!queue.empty()) {
            std::string_view query = queue.front();
            res = PQexec(_conn_for_send_query_backend, query.data());
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
//                std::cout << "send query backend failed" << std::endl;
//                fprintf(stderr, query.data(), PQerrorMessage(_conn_for_send_query_backend));
//                exit_nicely(conn);
            }
            queue.pop();
            PQclear(res);
        }

        /* トランザクションを終了する */
        res = PQexec(_conn_for_send_query_backend, "END");
        PQclear(res);
    }

    void bridge::handle_downstream_read(const boost::system::error_code &error,
                                        const size_t &bytes_transferred) {
        if (!error) {
//            std::cout << "handle downstream_read" << std::endl;
//            debug::hexdump(reinterpret_cast<const char *>(downstream_data_), bytes_transferred); // 下流バッファバッファ16進表示 test
            // ヘッダ情報の読み込み

            int n = 0;

            if (downstream_data_[0] == 0x51) { // 1バイト目が'Q'のとき
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

                auto transaction_status = res.front().status();
                if (transaction_status == transaction::Status::Commit) {
                    queue_sender.submit([&]() { send_queue_backend(query_queue); });
                }

                // 内部DBに結果がない場合は後ろにそのまま流しつつ、内部DBに結果を保存する
                if (transaction_status == transaction::Status::Select_Pending) {
//                    std::thread in_mem_updater([&]{
//                        download_result(*_conn, req, in_mem_db);
//                    });
                    async_write(upstream_socket_,
                                boost::asio::buffer(downstream_data_, bytes_transferred),
                                boost::bind(&bridge::handle_upstream_write,
                                            shared_from_this(),
                                            boost::asio::placeholders::error));
//                    in_mem_updater.join();
                } else if (transaction_status == transaction::Status::Result) {
                    std::basic_string<unsigned char> result;

                    auto Ds = res.front().get_results();
                    auto res_type_char = req.query().query()[7];
                    // ヘッダ情報のバイトパック
                    if (res_type_char == 'c' || res_type_char == 'D') {
                        result.append(response::sysbench_tbl_c_header);
                    } else if (res_type_char == 's' || res_type_char == 'S') {
                        result.append(response::sysbench_tbl_sum_header);
                    } else {
                        result.append(response::sysbench_tbl_header);
                    }
                    while (!Ds.empty()) {
                        auto res_type = Ds.front().index(); // 0 -> sysbench, 1 -> sysbench_one, 2 -> sysbench_sum
                        std::vector<unsigned char> D;
                        if (res_type == 0) { // sysbench
                            D = std::get<0>(Ds.front()).bytes();
                        } else if (res_type == 1) { // sysbench_one
                            D = std::get<1>(Ds.front()).bytes();
                        } else if (res_type == 2) { // sysbench_sum
                            D = std::get<2>(Ds.front()).bytes();
                        }
                        result.append(std::basic_string(D.begin(), D.end()));
                        Ds.pop();
                        if (result.size() > 60000) // バッファあふれの回避
                            break;
                    }
                    // フッタ情報の追加

                    result.append(response::sysbench_slct_cmd);
                    result.append(response::sysbench_status);
//                    debug::hexdump(reinterpret_cast<const char *>(result.data()), result.size()); // 下流バッファバッファ16進表示

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

                // postgresのParseメッセージ(PSの宣言)
            } else if (downstream_data_[0] == 0x50) {
                // Body部の計算 -> 2,3,4,5バイト目でクエリ全体のサイズ計算 -> クエリの種類と長さの情報を切り取る
                n = (int) downstream_data_[1] << 24;
                n += (int) downstream_data_[2] << 16;
                n += (int) downstream_data_[3] << 8;
                n += (int) downstream_data_[4];

//            debug::hexdump(reinterpret_cast<const char *>(downstream_data_), bytes_transferred); // 下流バッファバッファ16進表示
                int prepared_statement_id_length = 0;
                while (downstream_data_[prepared_statement_id_length + 5] !=
                       '\0') { // 5はメッセージ形式とメッセージ帳のバイト長だけ進めることを意味する
                    prepared_statement_id_length++;
                }
                auto prepared_statement_id = std::string(reinterpret_cast<const char *>(&downstream_data_[5]),
                                                         prepared_statement_id_length);
                auto prepared_statement_query = std::string(
                        reinterpret_cast<const char *>(&downstream_data_[5 + prepared_statement_id_length]),
                        n - 4 - prepared_statement_id_length);

                prepared_statements_lists.insert(std::make_pair(prepared_statement_id, prepared_statement_query));

                for (auto &prepared_statements_list: prepared_statements_lists) {
                    std::cout << "itr test :: first_id: " << prepared_statements_list.first << " second_query: "
                              << prepared_statements_list.second << std::endl;
                }
                async_write(upstream_socket_,
                            boost::asio::buffer(downstream_data_, bytes_transferred),
                            boost::bind(&bridge::handle_upstream_write,
                                        shared_from_this(),
                                        boost::asio::placeholders::error));
            } else {
                async_write(upstream_socket_,
                            boost::asio::buffer(downstream_data_, bytes_transferred),
                            boost::bind(&bridge::handle_upstream_write,
                                        shared_from_this(),
                                        boost::asio::placeholders::error));
            }


//            auto download_end = std::chrono::system_clock::now();
//            auto msec = std::chrono::duration_cast<std::chrono::milliseconds>(download_end - download_start).count();
////                    std::cout << "download_time:" << (download_end - download_start).count() << std::endl;
//            std::cout << "handle_downstream_read time:" << msec << std::endl;

        } else { // if (!error)

            // error の内容を表示
            std::cout << "Error code: " << error.value() << std::endl;
            std::cout << "Error message: " << error.message() << std::endl;
//            close_and_reset();
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
            close_and_reset();
        }
    }

    void bridge::close_and_reset() {
        boost::mutex::scoped_lock lock(mutex_);

        if (downstream_socket_.is_open()) {
            downstream_socket_.close();
        }

        if (upstream_socket_.is_open()) {
            upstream_socket_.close();
        }
        bridge::start(upstream_host_, upstream_port_);
    }

}// namespace tcp_proxy
