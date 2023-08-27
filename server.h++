//
// Created by mint25mt on 2022/08/18.
//
#pragma once

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include <cassandra.h>
#include <libpq-fe.h>
#include <queue>
#include <iostream>
#include "src/ThreadPool/ThreadPool.h++"
#include "query.h++"
//#include "src/reqestresponse/Request.h++"
#include <sqlite3.h>
#include <iomanip>


namespace tcp_proxy {
    namespace ip = boost::asio::ip;

    class bridge : public boost::enable_shared_from_this<bridge> {
        typedef ip::tcp::socket socket_type;
        typedef boost::shared_ptr<bridge> ptr_type;
    public:
        explicit bridge(boost::asio::io_service &ios, unsigned short upstream_port, std::string upstream_host, sqlite3*& in_mem_db);

        ~bridge();

        socket_type &downstream_socket();

        socket_type &upstream_socket();

        void start(const std::string &upstream_host, unsigned short upstream_port);

        void close_and_reset();

        void handle_upstream_connect(const boost::system::error_code &error);
        /*
            Section B: Client --> Proxy --> Remove Server
            Process data recieved from client then write to remove server.
        */

        // Read from client complete, now send data to remote server
        void handle_downstream_read(const boost::system::error_code &error,
                                    const size_t &bytes_transferred);

        // Write to remote server complete, Async read from client
        void handle_upstream_write(const boost::system::error_code &error);

        static void send_queue_backend(std::queue<std::string> &queue);

        socket_type upstream_socket_;

    private:

        void handle_upstream_read(const boost::system::error_code &error, const size_t &bytes_transferred);

        void handle_downstream_write(const boost::system::error_code &error);

        void handle_downstream_write_proxy(const boost::system::error_code &error);

//        void download_result(PGconn &conn, const transaction::Request &req, sqlite3 *in_mem_db);
        static void
        exit_nicely(PGconn *conn) {
            PQfinish(conn);
            exit(1);
        }

        void connectToPostgres(PGconn *_conn, const char *backend_postgres_connInfo) {
            _conn = PQconnectdb(backend_postgres_connInfo);
            if (PQstatus(_conn) != CONNECTION_OK) {
                fprintf(stderr, "Connection to database failed: %s",
                        PQerrorMessage(_conn));
                exit_nicely(_conn);
            }
        };

        // Execute SQL against SQLite
        bool executeSQLite(sqlite3 *db, const char *sql) {
            char *errMsg = nullptr;
            if (sqlite3_exec(db, sql, 0, 0, &errMsg) != SQLITE_OK) {
                std::cerr << "SQLite error: " << errMsg << std::endl;
                sqlite3_free(errMsg);
                return false;
            }
            return true;
        }

// Insert data from a PostgreSQL query result into an SQLite table
        bool insertDataFromResult(PGconn *_conn, sqlite3 *in_mem_db, PGresult *res, const char* tableName) {
            int nFields = PQnfields(res);
            for (int i = 0; i < PQntuples(res); i++) {
                std::string insertQuery = "INSERT OR REPLACE INTO " + std::string(tableName) + " VALUES(";
                for (int j = 0; j < nFields; j++) {
                    if (j > 0) {
                        insertQuery += ",";
                    }
                    insertQuery += "'" + std::string(PQgetvalue(res, i, j)) + "'";
                }
                insertQuery += ");";

                if (!executeSQLite(in_mem_db, insertQuery.c_str())) {
                    return false;
                }
            }
            return true;
        }

// Fetch data from PostgreSQL and insert into SQLite
        bool migrateTableData(PGconn *_conn, sqlite3 *in_mem_db, const char* tableName) {
            std::string query = "SELECT * FROM " + std::string(tableName);
            PGresult *res = PQexec(_conn, query.c_str());

            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                std::cerr << "Failed to fetch data from PostgreSQL for table: " << PQerrorMessage(_conn) << std::endl;
                PQclear(res);
                return false;
            }

            bool success = insertDataFromResult(_conn, in_mem_db, res, tableName);

            PQclear(res);
            return success;
        }

        std::string replacePlaceholders(const std::string &query, const std::vector<std::string> &params);

        uint32_t extractBigEndian4Bytes(const unsigned char *data, size_t &start) {
            uint32_t val = (data[start] << 24) |
                           (data[start + 1] << 16) |
                           (data[start + 2] << 8) |
                           data[start + 3];
            start += 4;
            return val;
        };

        uint16_t extractBigEndian2Bytes(const unsigned char *data, size_t &index) {
            uint16_t val = (data[index] << 8) |
                           data[index + 1];
            index += 2;
            return val;
        };

        void
        processParseMessage(size_t &index, const size_t &bytes_transferred, std::queue<unsigned char> &clientQueue,
                            std::queue<transaction::Query> &queryQueue,
                            std::queue<std::queue<int>> &column_format_codes
        ) {
            size_t first_index = index;
            uint32_t message_size = extractBigEndian4Bytes(downstream_data_, index);
//            std::cout << "parse message_size : " << message_size << std::endl;

            std::string statement_id = extractString(downstream_data_, index);
//            std::cout << "statement id : " << statement_id << std::endl;

            std::string query = extractString(downstream_data_, index);

//        // 初期化とか設定のクエリはスキップ
//        if (should_skip_query(query)) {
//            std::cout << "skip query: " << query << std::endl;
//
//            async_write(upstream_socket_,
//                        boost::asio::buffer(downstream_data_, bytes_transferred),
//                        boost::bind(&bridge::handle_upstream_write,
//                                    shared_from_this(),
//                                    boost::asio::placeholders::error));
//            return;
//        }
            if (!query.empty()) {
                std::cout << "parse query: " << query << std::endl;
//                for (unsigned char byte: query) {
//                    std::cout << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte) << " ";
//                }
            }

            if (!statement_id.empty()) {
                prepared_statements_lists[statement_id] = query;
            }
            index = first_index + message_size;
            std::cout << "next message parse: " << downstream_data_[index] << std::endl;

            if (downstream_data_[index] == 'B') {
                clientQueue.push('B');
                index++;
                processBindMessage(index, bytes_transferred, clientQueue, query, queryQueue, column_format_codes);
            } else if (downstream_data_[index] == 'D') {
                clientQueue.push('D');
                index++;
                processDescribeMessage(index, bytes_transferred, clientQueue, query, queryQueue, column_format_codes);
            } else {
                index = 0;
            }
        };


        // <パラメータの総数(2byte)><パラメータの型(2byte)><パラメータの総数(2byte)><<パラメータサイズ(4byte),データ(パラメータサイズ)>>
        // 呼び出す前にメッセージ形式の最初は処理済みであること('B'の次をindexが指していることがのぞましい)
        void processBindMessage(size_t &index, const size_t &bytes_transferred, std::queue<unsigned char> &clientQueue,
                                std::string query, std::queue<transaction::Query> &queryQueue,
                                std::queue<std::queue<int>> &column_format_codes) {
            size_t first_index = index;

            uint32_t message_size = extractBigEndian4Bytes(downstream_data_, index);

            // portal
            index += 1;

            std::string statement_id = extractString(downstream_data_, index);

//        std::string query;
            // region statementIDがある時のバインド処理
            if (!statement_id.empty() && prepared_statements_lists.count(statement_id)) {
                query = prepared_statements_lists[statement_id];
            }

            // クエリをスキップする必要があるかの変数、Bindパラメータが100を超えるとバッファ落ちするため
            bool should_unfold_bind_parameter = true;
            //INSERT INTO OORDER_LINE が含まれるとき
            if (query.find("INSERT INTO ORDER_LINE") != std::string::npos) {
                query = " INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES(3064,1,1,1,64103,1,7,1324.1,tejhzqnualvixeerulnkbvd ), (3064,1,1,2,72919,1,2,,efwojvfutgooqydegbckgqd ), (3064,1,1,1,64103,1,7,1324.1,tejhzqnualvixeerulnkbvd ), (3064,1,1,2,72919,1,2,,efwojvfutgooqydegbckgqd ), (3064,1,1,3,64103,1,7,1324.1,tejhzqnualvixeerulnkbvd), (3064,1,1,4,64103,1,7,1324.1,tejhzqnualvixeerulnkbvd), (3064,1,1,5,64103,1,7,1324.1,tejhzqnualvixeerulnkbvd), (3064,1,1,6,64103,1,7,1324.1,tejhzqnualvixeerulnkbvd), (3064,1,1,7,64103,1,7,1324.1,tejhzqnualvixeerulnkbvd), (3064,1,1,8,64103,1,7,1324.1,tejhzqnualvixeerulnkbvd), (3064,1,1,9,64103,1,7,1324.1,tejhzqnualvixeerulnkbvd), (3064,1,1,10,64103,1,7,1324.1,tejhzqnualvixeerulnkbvd), (3064,1,1,11,64103,1,7,1324.1,tejhzqnualvixeerulnkbvd)";
                should_unfold_bind_parameter = false;
            }

            if (should_unfold_bind_parameter) {
                uint16_t num_parameters = extractBigEndian2Bytes(downstream_data_, index);

                std::vector<uint16_t> type_parameters(num_parameters);

                for (uint16_t i = 0; i < num_parameters; ++i) {
                    type_parameters[i] = extractBigEndian2Bytes(downstream_data_, index);
                    std::cout << "type_parameter : " << type_parameters[i] << std::endl;
                }

                std::cout << "num parameters : " << num_parameters << std::endl;

                index += 2; // parameter value の先頭2バイトをスキップ(parameter_formatsと同義)

                std::vector<std::string> datas;
                for (uint16_t i = 0; i < num_parameters; ++i) {
                    // kimoi
                    uint32_t column_length = extractBigEndian4Bytes(downstream_data_, index);
                    if (type_parameters[i] == 0) {
                        // Text format
                        std::string value = extractString(downstream_data_, index);
                        datas.emplace_back(value);
                        // インデックスの更新
//                    index += data_size;
                    } else if (type_parameters[i] == 1) {
                        uint32_t value = extractBigEndian4Bytes(downstream_data_, index);
                        std::cout << "detect paramter as binary in bind . value is : " << value << std::endl;
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

                // キューにクエリを追加
                queryQueue.push(transaction::Query(query));

                // column_format_code
                std::queue<int> column_format_code;

                // results_formatsの2バイトをintに変換し、int results_formatsに格納する
//            std::cout << "num_result_format_bytes[0]" << std::hex << std::setw(2) << std::setfill('0')
//                                                         << static_cast<int>(downstream_data_[index])
//                                                         << std::endl;
//            std::cout << "num_result_format_bytes[1]" << std::hex << std::setw(2) << std::setfill('0')
//                      << static_cast<int>(downstream_data_[index + 1])
//                      << std::endl;

                uint16_t num_result_formats = extractBigEndian2Bytes(downstream_data_, index);

                std::cout << "num_result_format : " << num_result_formats << std::endl;
                for (int i = 0; i < num_result_formats; ++i) {
                    uint16_t format_code = extractBigEndian2Bytes(downstream_data_, index);
                    std::cout << "format_code : " << format_code << std::endl;
                    column_format_code.push(format_code);
                }

                column_format_codes.push(column_format_code);
            } // should_unfold_bind_parameter

            index = first_index + message_size; // 1 + message_size + 1
            std::cout << "next message bind: " << downstream_data_[index] << std::endl;

            if (downstream_data_[index] == 'D') {
                clientQueue.push('D');
                index++;
                processDescribeMessage(index, bytes_transferred, clientQueue, query, queryQueue, column_format_codes);
            } else if (downstream_data_[index] == 'E') {
                clientQueue.push('E');
                index++;
                processExecuteMessage(index, bytes_transferred, clientQueue, query, queryQueue, column_format_codes);
            }
        }

        void
        processDescribeMessage(size_t &index, const size_t &bytes_transferred, std::queue<unsigned char> &clientQueue,
                               std::string &query, std::queue<transaction::Query> &queries,
                               std::queue<std::queue<int>> &column_format_codes) {
//            std::cout << "processDesc" << std::endl;
            uint32_t message_size = extractBigEndian4Bytes(downstream_data_, index);

            unsigned char ps_or_s = downstream_data_[index];
            index++;

            std::string ps_or_portal_name = extractString(downstream_data_, index);

            std::cout << "next message desc: " << downstream_data_[index] << std::endl;
            if (downstream_data_[index] == 'E') {
                clientQueue.push('E');
                index++;
                processExecuteMessage(index, bytes_transferred, clientQueue, query, queries, column_format_codes);
            } else if (downstream_data_[index] == 'B') {
                clientQueue.push('B');
                index++;
                processBindMessage(index, bytes_transferred, clientQueue, query, queries, column_format_codes);
            }
        }

        void
        processExecuteMessage(size_t &index, const size_t &bytes_transferred, std::queue<unsigned char> &clientQueue,
                              std::string &query,
                              std::queue<transaction::Query> &queries,
                              std::queue<std::queue<int>> &column_format_codes) {
            uint32_t message_size = extractBigEndian4Bytes(downstream_data_, index);

            std::string portal_name = extractString(downstream_data_, index);

            uint16_t return_row_size = extractBigEndian4Bytes(downstream_data_, index);

            std::cout << "next message execute: " << downstream_data_[index] << std::endl;
            if (downstream_data_[index] == 'S') {
                clientQueue.push('S');
                index++;
                processSyncMessage(index, bytes_transferred, clientQueue, query, queries, column_format_codes);
            } else if (downstream_data_[index] == 'P') {
                clientQueue.push('P');
                index++;
                processParseMessage(index, bytes_transferred, clientQueue, queries, column_format_codes);
            } else if (downstream_data_[index] == 'B') {
                clientQueue.push('B');
                index++;
                processBindMessage(index, bytes_transferred, clientQueue, query, queries, column_format_codes);
            } else if (downstream_data_[index] == 'D') {
                clientQueue.push('D');
                index++;
                processDescribeMessage(index, bytes_transferred, clientQueue, query, queries, column_format_codes);
            }
        };

        void processSyncMessage(size_t &index, const size_t &bytes_transferred, std::queue<unsigned char> &clientQueue,
                                std::string &query,
                                std::queue<transaction::Query> &queries,
                                std::queue<std::queue<int>> &column_format_codes);

        std::string extractString(const unsigned char *data, size_t &start) {
            size_t end = start;
            while (data[end] != 0x00) {
                ++end;
            }
            std::string result(reinterpret_cast<const char *>(&data[start]), end - start);
            start = end + 1; // Adjust to position after the 0x00 byte
            return result;
        };

        void printStatements();

        void dumpString(const std::string &str);

        bool should_skip_query(const std::string &data);


        socket_type downstream_socket_;

        enum {
            max_data_length = 8192 * 2
        }; //16KB
        unsigned char downstream_data_[max_data_length];
        unsigned char upstream_data_[max_data_length];
        boost::mutex mutex_;
        static constexpr char *backend_host = "192.168.12.17";
        static constexpr char *backend_postgres_conninfo = "host=192.168.12.17 port=5433 dbname=yugabyte user=yugabyte password=yugabyte";
        unsigned short upstream_port_;
        std::string upstream_host_;
        std::shared_ptr<CassFuture> _connectFuture;
        std::shared_ptr<CassCluster> _cluster;
        std::shared_ptr<CassSession> _session;
        PGconn *_conn;
        std::queue<std::string> query_queue;
        std::mutex queue_mtx;
        pool::ThreadPoolExecutor queue_sender;
        sqlite3 *_in_mem_db;
        std::map<std::string, std::string> prepared_statements_lists;
        static constexpr char *write_ahead_log = "/home/y-watanabe/wal.csv";
        static constexpr char *text_create_tbl_bench = "create table if not exists bench (pk text primary key, field1 integer, field2 integer, field3 integer)";
        static constexpr char *text_create_tbl_sbtest1 = "create table if not exists sbtest1 (id integer primary key, k integer, c text, pad text)";
        static constexpr char *text_download_sbtest1 = "select * from sbtest1";



        // inner class
    public:
        class acceptor final {
        public:
            explicit acceptor(boost::asio::io_service &io_service,
                              const std::string &local_host,
                              unsigned short local_port,
                              const std::string &upstream_host,
                              unsigned short upstream_port,
                              sqlite3*& in_mem_db
                              );

            ~acceptor();

            bool accept_connections();

        private:
            void handle_accept(const boost::system::error_code &error);

            boost::asio::io_service &io_service_;
            ip::address_v4 localhost_address;
            ip::tcp::acceptor acceptor_;
            ptr_type session_;
            unsigned short upstream_port_;
            std::string upstream_host_;
            sqlite3 *in_mem_db_;
        };
    };
}