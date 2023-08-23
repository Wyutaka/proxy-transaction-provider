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

        // postgresのコネクション
        _conn = PQconnectdb(backend_postgres_conninfo);
        /* バックエンドとの接続確立に成功したかを確認する */
        if (PQstatus(_conn) != CONNECTION_OK) {
            fprintf(stderr, "Connection to database failed: %s",
                    PQerrorMessage(_conn));
            exit_nicely(_conn);
        }

//        bridge::connectToPostgres(_conn, backend_postgres_conninfo);
        bridge::initializeSQLite();
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
    void bridge::initializeSQLite() {
        sqlite3 *in_mem_db;
        int ret = sqlite3_open(":memory:", &in_mem_db);
        if (ret != SQLITE_OK) {
            std::cout << "FILE OPEN Error";
            close_and_reset();
        }

        ret = sqlite3_exec(in_mem_db, text_create_tbl_sbtest1,
                           NULL, NULL, NULL);
        ret = sqlite3_exec(in_mem_db, text_create_OORDER,
                           NULL, NULL, NULL);
        ret = sqlite3_exec(in_mem_db, text_create_DISTRICT,
                           NULL, NULL, NULL);
        ret = sqlite3_exec(in_mem_db, text_create_ITEM,
                           NULL, NULL, NULL);
        ret = sqlite3_exec(in_mem_db, text_create_WAREHOUSE,
                           NULL, NULL, NULL);
        ret = sqlite3_exec(in_mem_db, text_create_CUSTOMER,
                           NULL, NULL, NULL);
        ret = sqlite3_exec(in_mem_db, text_create_ORDER_LINE,
                           NULL, NULL, NULL);
        ret = sqlite3_exec(in_mem_db, text_create_NEW_ORDER,
                           NULL, NULL, NULL);
        ret = sqlite3_exec(in_mem_db, text_create_STOCK,
                           NULL, NULL, NULL);
        ret = sqlite3_exec(in_mem_db, text_create_HISTORY,
                           NULL, NULL, NULL);
        std::cout << "test" << std::endl;

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
//            debug::hexdump(reinterpret_cast<const char *>(downstream_data_), 256); // 下流バッファバッファ16進表示 test
            // ヘッダ情報の読み込み

            // 設定くえりはむし
//            if (!should_skip_query(query)) {
//                async_write(upstream_socket_,
//                            boost::asio::buffer(data, bytes_transferred),
//                            boost::bind(&bridge::handle_upstream_write,
//                                        shared_from_this(),
//                                        boost::asio::placeholders::error));
//            }

            // クライアントのメッセージ形式を保存するキュー
            std::queue<unsigned char> clientQueue;

            // 1バイト目が'Q'のとき
            if (downstream_data_[0] == 0x51) {

                clientQueue.push('Q');
//                std::cout << "handle downstream_read" << std::endl;
                size_t index = 1;
                // Body部の計算 -> 2,3,4,5バイト目でクエリ全体のサイズ計算 -> クエリの種類と長さの情報を切り取る
                size_t message_size_q = extractBigEndian4Bytes(downstream_data_, index);

                // lock層の生成(postgres用)
                transaction::lock::Lock<transaction::PostgresConnector> lock{
                        transaction::PostgresConnector(transaction::PostgresConnector(shared_from_this(), _conn))
                };

//                std::cout << "query " << std::endl;

                std::string query = extractString(downstream_data_, index);

//                std::cout << "create req " << std::endl;
                // リクエストの生成
                const transaction::Request &req = transaction::Request(
                        transaction::Peer(upstream_host_, upstream_port_), query, clientQueue); // n-4 00まで含める｀h

//                std::cout << "create res" << std::endl;
                // レスポンス生成
                const auto &res = lock(req, write_ahead_log, query_queue, in_mem_db);

//                std::cout << "get_res_end" << std::endl;
                // フロントエンドにresponse_bufferを送信
                std::vector<unsigned char> response_buffer = res.back().get_raw_response();

//                std::cout << "get response_buffer" << std::endl;

//                for (unsigned char byte : response_buffer) {
//                    std::cout << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte) << " ";
//                }
//                std::cout << std::endl;

                async_write(downstream_socket_,
                            boost::asio::buffer(response_buffer.data(), response_buffer.size()), // result_okの文字列長
                            boost::bind(&bridge::handle_downstream_write,
                                        shared_from_this(),
                                        boost::asio::placeholders::error));

                // クライアントからの読み込みを開始 (downstream)
                downstream_socket_.async_read_some(
                        boost::asio::buffer(downstream_data_, max_data_length),
                        boost::bind(&bridge::handle_downstream_read,
                                    shared_from_this(),
                                    boost::asio::placeholders::error,
                                    boost::asio::placeholders::bytes_transferred));

            }
                // postgresのBindメッセージ(PSのバインド)
            else if (downstream_data_[0] == 0x42) {
                size_t index = 1;
                clientQueue.push('B');
                // statementidがあるはずh
                processBindMessage(index, bytes_transferred, clientQueue, "");

                // とりあえず後ろに流す TODO 多分いらない
//                async_write(upstream_socket_,
//                            boost::asio::buffer(downstream_data_, bytes_transferred),
//                            boost::bind(&bridge::handle_upstream_write,
//                                        shared_from_this(),
//                                        boost::asio::placeholders::error));

            }
                // postgresのParseメッセージ(PSの宣言)
            else if (downstream_data_[0] == 0x50) {
                size_t index = 1;
                clientQueue.push('P');
                processParseMessage(index, bytes_transferred, clientQueue);

                // とりあえず後ろに流す TODO 多分いらない
//                async_write(upstream_socket_,
//                            boost::asio::buffer(downstream_data_, bytes_transferred),
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

//            auto download_end = std::chrono::system_clock::now();
//            auto msec = std::chrono::duration_cast<std::chrono::milliseconds>(download_end - download_start).count();
////                    std::cout << "download_time:" << (download_end - download_start).count() << std::endl;
//            std::cout << "handle_downstream_read time:" << msec << std::endl;

        } else { // if (!error)
            // error の内容を表示
            std::cout << "handle_download_read" << error.value() << std::endl;
            std::cout << "Error code: " << error.value() << std::endl;
            std::cout << "Error message: " << error.message() << std::endl;
//            close_and_reset();
        }
    }

    // クエリが無視リストに含まれているか確認
//    bool bridge::should_skip_query(const std::string &data) {
//        if (data.empty() || data.find("SET") != std::string::npos || data.find("SHOW") != std::string::npos
//                                                                     || data.find("BEGIN") != std::string::npos
//                                                                        || data.find("ROLLBACK") != std::string::npos) {
//            return true;
//        }
//        return false;
//    }

    // クエリが無視リストに含まれているか確認
    bool bridge::should_skip_query(const std::string &data) {
        const std::string ignore_queries[] = {
                "SET extra_float_digits = 3",
                "SET application_name = 'PostgreSQL JDBC Driver'",
//                "",  // 空文字
                "SHOW TRANSACTION ISOLATION LEVEL",
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                "BEGIN",
                "ROLLBACK"
        };

        for (const auto &query: ignore_queries) {
            if (data.find(query) != std::string::npos || data == "") {
                return true;
            }
        }
        return false;
    }

    // <パラメータの総数(2byte)><パラメータの型(2byte)><パラメータの総数(2byte)><<パラメータサイズ(4byte),データ(パラメータサイズ)>>
    // 呼び出す前にメッセージ形式の最初は処理済みであること('B'の次をindexが指していることがのぞましい)
    void
    bridge::processBindMessage(size_t &index, const size_t &bytes_transferred, std::queue<unsigned char> &clientQueue,
                               std::string query) {
        size_t first_index = index;

        uint32_t message_size = extractBigEndian4Bytes(downstream_data_, index);

        index += 1;
        std::string statement_id = extractString(downstream_data_, index);

//        std::string query;
        // region statementIDがある時のバインド処理
        if (!statement_id.empty() && prepared_statements_lists.count(statement_id)) {
            query = prepared_statements_lists[statement_id];
        }

        uint16_t num_parameters = extractBigEndian2Bytes(downstream_data_, index);

        std::vector<uint16_t> type_parameters(num_parameters);

        for (uint16_t i = 0; i < num_parameters; ++i) {
            type_parameters[i] = extractBigEndian2Bytes(downstream_data_, index);
        }

        index += 2; // parameter value の先頭2バイトをスキップ
        std::vector<std::string> datas;
        for (uint16_t i = 0; i < num_parameters; ++i) {
            uint32_t data_size = extractBigEndian4Bytes(downstream_data_, index);
            if (type_parameters[i] == 0) {
                // Text format
                std::string value = extractString(downstream_data_, index);
                datas.emplace_back(value); // インデックスの更新


                index += data_size;
            } else if (type_parameters[i] == 1) {
                // Binary format - for simplicity, we're assuming it's a 4-byte integer.
                // A real proxy should handle all binary types properly.
                uint32_t value = extractBigEndian4Bytes(downstream_data_, index);

                datas.push_back(std::to_string(value));
            }
        }

        for (size_t i = 0; i < datas.size(); ++i) {
            std::string placeholder = "$" + std::to_string(i + 1);
            size_t pos = query.find(placeholder);
            if (pos != std::string::npos) {
                query.replace(pos, placeholder.length(), datas[i]);
            }
        }
        // end region statementIDがある時のバインド処理


        std::cout << "Bind query: " << query << std::endl;

        index = first_index + message_size; // 1 + message_size + 1
        std::cout << "next message bind: " << downstream_data_[index] << std::endl;

        if (downstream_data_[index] == 'D') {
            clientQueue.push('D');
            index++;
            processDescribeMessage(index, bytes_transferred, clientQueue, query);
        } else if (downstream_data_[index] == 'E') {
            clientQueue.push('E');
            index++;
            processExecuteMessage(index, bytes_transferred, clientQueue, query);
        }


//        debug::hexdump(reinterpret_cast<const char *>(downstream_data_), 128); // 下流バッファバッファ16進表示 test

    }

    void
    bridge::processSyncMessage(size_t &index, const size_t &bytes_transferred, std::queue<unsigned char> &clientQueue,
                               std::string &query) {
        uint32_t message_size = extractBigEndian4Bytes(downstream_data_, index);

        std::cout << "next message processSync: 0x"
                  << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<int>(downstream_data_[index] & 0xFF)
                  << std::dec << downstream_data_[index]  << std::endl;

        // lock層の生成(postgres用)
        transaction::lock::Lock<transaction::PostgresConnector> lock{
                transaction::PostgresConnector(transaction::PostgresConnector(shared_from_this(), _conn))
        };

        // リクエストの生成
        const transaction::Request &req = transaction::Request(transaction::Peer(upstream_host_, upstream_port_), query,
                                                               clientQueue); // n-4 00まで含める｀h

        // レスポンス生成
        const auto &res = lock(req, write_ahead_log, query_queue, in_mem_db);

        std::cout << "after lock" << std::endl;

        // write backend (返却用バッファに追加)

//        index += message_size;

        if (downstream_data_[index] == 'P') {
            clientQueue.push('P');
            index++;
            std::cout << "p" << std::endl;
            processParseMessage(index, bytes_transferred, clientQueue);
        } else if (downstream_data_[index] == 'B') {
            clientQueue.push('B');
            index++;
            std::cout << "B" << std::endl;

            // statemtn_idがある
            processBindMessage(index, bytes_transferred, clientQueue, "");
        } else {
            std::cout << "write to backend" << std::endl;

            // TODO write to backend
            // フロントエンドにresponse_bufferを送信
            auto response_buffer = res.back().get_raw_response();

            std::cout << "get response_buffer" << std::endl;

            for (unsigned char byte: response_buffer) {
                std::cout << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte) << " ";
            }
            std::cout << std::endl;

            async_write(downstream_socket_,
                        boost::asio::buffer(response_buffer.data(), response_buffer.size()), // result_okの文字列長
                        boost::bind(&bridge::handle_downstream_write,
                                    shared_from_this(),
                                    boost::asio::placeholders::error));

            // フロントエンドからの読み込みを開始
            downstream_socket_.async_read_some(
                    boost::asio::buffer(downstream_data_, max_data_length),
                    boost::bind(&bridge::handle_downstream_read,
                                shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));
        }
    }

    uint32_t bridge::extractBigEndian4Bytes(const unsigned char *data, size_t &start) {
        uint32_t val = (data[start] << 24) |
                       (data[start + 1] << 16) |
                       (data[start + 2] << 8) |
                       data[start + 3];
        start += 4;
        return val;
    }

    uint16_t bridge::extractBigEndian2Bytes(const unsigned char *data, size_t &index) {
        uint16_t val = (data[index] << 8) |
                       data[index + 1];
        index += 2;
        return val;
    }

    void bridge::processParseMessage(size_t &index, const size_t &bytes_transferred,
                                     std::queue<unsigned char> &clientQueue) {
        size_t first_index = index;
        uint32_t message_size = extractBigEndian4Bytes(downstream_data_, index);

        std::string statement_id = extractString(downstream_data_, index);

        std::string query = extractString(downstream_data_, index);

        // 初期化とか設定のクエリはスキップ
        if (should_skip_query(query)) {
            std::cout << "skip query: " << query << std::endl;

            async_write(upstream_socket_,
                        boost::asio::buffer(downstream_data_, bytes_transferred),
                        boost::bind(&bridge::handle_upstream_write,
                                    shared_from_this(),
                                    boost::asio::placeholders::error));
            return;
        }

        std::cout << "parse query: " << query << std::endl;

        if (!statement_id.empty()) {
            prepared_statements_lists[statement_id] = query;
        }

        index = first_index + message_size;
        std::cout << "next message parse: " << downstream_data_[index] << std::endl;

        if (downstream_data_[index] == 'B') {
            std::cout << "goto BIND:" << query << std::endl;
            clientQueue.push('B');
            index++;
            processBindMessage(index, bytes_transferred, clientQueue, query);
        } else {
            index = 0;
        }
    }

    void bridge::dumpString(const std::string &str) {
        for (char c: str) {
            // setw(2)とsetfill('0')は、ヘックスで1桁の場合に2桁になるよう0で埋めたもの
            std::cout << std::hex << std::setw(2) << std::setfill('0') << (int) (unsigned char) c << " ";
        }
        std::cout << std::endl;
    }


    void bridge::printStatements() {
        for (const auto &pair: prepared_statements_lists) {
            std::cout << "Statement ID: " << pair.first << " Query: " << pair.second << std::endl;
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
