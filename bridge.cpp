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

//int callback(void *notUsed, int argc, char **argv, char **azColName) {
//    for (int i = 0; i < argc; i++) {
//        std::cout << azColName[i] << " = " << (argv[i] ? argv[i] : "NULL") << std::endl;
//    }
//    std::cout << std::endl;
//    return 0;
//}

namespace tcp_proxy {
    typedef ip::tcp::socket socket_type;
    typedef boost::shared_ptr<bridge> ptr_type;

    bridge::bridge(boost::asio::io_service &ios,
                   unsigned short upstream_port,
                   std::string upstream_host,
                   sqlite3*& in_mem_db)
            : downstream_socket_(ios),
              upstream_socket_(ios),
              upstream_host_(upstream_host),
              upstream_port_(upstream_port),
              _connectFuture(NULL),
              _cluster(CASS_SHARED_PTR(cluster, cass_cluster_new())),
              _session(std::shared_ptr<CassSession>(cass_session_new(), transaction::detail::SessionDeleter())),
              queue_sender(pool::ThreadPoolExecutor(1)),
              _in_mem_db(in_mem_db) {

        // postgresのコネクション
        _conn = PQconnectdb(backend_postgres_conninfo);
        /* バックエンドとの接続確立に成功したかを確認する */
        if (PQstatus(_conn) != CONNECTION_OK) {
            fprintf(stderr, "Connection to database failed: %s",
                    PQerrorMessage(_conn));
            exit_nicely(_conn);
        }

//        const char *sql = "SELECT D_NEXT_O_ID   FROM DISTRICT WHERE D_W_ID = 4    AND D_ID = 1";
//        char *zErrMsg = 0;
//        int rc = sqlite3_exec(in_mem_db, sql, callback, 0, &zErrMsg);
//        if (rc != SQLITE_OK) {
//            std::cerr << "error code : " << rc << std::endl;
//            std::cerr << "SQL error: " << zErrMsg << std::endl;
//            sqlite3_free(zErrMsg);
//        } else {
//            std::cout << "Operation done successfully" << std::endl;
//        }

        std::cout << "aaaa" << std::endl;
        bridge::connectToPostgres(_conn, backend_postgres_conninfo);
//        bridge::fetchAndCacheData(_conn, in_mem_db, text_download_sbtest1);
    }

    bridge::~bridge() {
        _connectFuture.reset();
        _session.reset();
        _cluster.reset();
        sqlite3_close(_in_mem_db);
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
            // クエリを複数保存するキュー
            std::queue<transaction::Query> queryQueue;
            // 結果列書式コードを保存するキュー
            std::queue<std::queue<int>> column_format_codes;

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
                        transaction::Peer(upstream_host_, upstream_port_), transaction::Query(query),
                        clientQueue); // n-4 00まで含める｀h

//                std::cout << "create res" << std::endl;
                // レスポンス生成
                const auto &res = lock(req, write_ahead_log, query_queue, _in_mem_db);

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
                // statementidがあるはず
                processBindMessage(index, bytes_transferred, clientQueue, "", queryQueue, column_format_codes);

            }
                // postgresのParseメッセージ(PSの宣言)
            else if (downstream_data_[0] == 0x50) {
                size_t index = 1;
                clientQueue.push('P');
                processParseMessage(index, bytes_transferred, clientQueue, queryQueue, column_format_codes);

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

    void
    bridge::processSyncMessage(size_t &index, const size_t &bytes_transferred,
                               std::queue<unsigned char> &clientQueue,
                               std::string &query, std::queue<transaction::Query> &queries,
                               std::queue<std::queue<int>> &column_format_codes) {
        uint32_t message_size = extractBigEndian4Bytes(downstream_data_, index);



        // lock層の生成(postgres用)
        transaction::lock::Lock<transaction::PostgresConnector> lock{
                transaction::PostgresConnector(transaction::PostgresConnector(shared_from_this(), _conn))
        };

        // リクエストの生成
        const transaction::Request &req = transaction::Request(transaction::Peer(upstream_host_, upstream_port_),
                                                               queries,
                                                               clientQueue,
                                                               column_format_codes); // n-4 00まで含める｀h

        // レスポンス生成
        // query_queue ???
        const auto &res = lock(req, write_ahead_log, query_queue, _in_mem_db);

        // write backend (返却用バッファに追加)

//        index += message_size;

        std::cout << "next message processSync: 0x"
                  << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<int>(downstream_data_[index] & 0xFF)
                  << std::dec << " (" << downstream_data_[index] << ")" << std::endl;

//        if (downstream_data_[index] == 'P') {
//            clientQueue.push('P');
//            index++;
//            processParseMessage(index, bytes_transferred, clientQueue, queries, column_format_codes);
//        } else if (downstream_data_[index] == 'B') {
//            clientQueue.push('B');
//            index++;
//
//            // statemtn_idがある
//            // Bindを最初に受け取った場合はstatementIDが必ずあるはずなので、””をクエリにする
//            processBindMessage(index, bytes_transferred, clientQueue, "", queries, column_format_codes);
//        }
//        else if (downstream_data_[index] == 'E') {
//            clientQueue.push('E');
//            index++;
//            processExecuteMessage(index, bytes_transferred, clientQueue, query, queries, column_format_codes);
//        }
//        else {
        std::cout << "write to backend" << std::endl;

        auto response_buffer = res.back().get_raw_response();

        async_write(downstream_socket_,
                    boost::asio::buffer(response_buffer.data(), response_buffer.size()), // result_okの文字列長
                    boost::bind(&bridge::handle_downstream_write,
                                shared_from_this(),
                                boost::asio::placeholders::error));

        response_buffer;

        // フロントエンドからの読み込みを開始
        downstream_socket_.async_read_some(
                boost::asio::buffer(downstream_data_, max_data_length),
                boost::bind(&bridge::handle_downstream_read,
                            shared_from_this(),
                            boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred));
//        }
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
