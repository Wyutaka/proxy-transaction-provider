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


namespace tcp_proxy {
    namespace ip = boost::asio::ip;

    class bridge : public boost::enable_shared_from_this<bridge> {
        typedef ip::tcp::socket socket_type;
        typedef boost::shared_ptr<bridge> ptr_type;
    public:
        explicit bridge(boost::asio::io_service &ios, unsigned short upstream_port, std::string upstream_host);

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
        void connectToPostgres(PGconn *_conn, const char *backend_postgres_connInfo);

        void initializeSQLite();

        void fetchAndCacheData(PGconn *_conn, sqlite3 *in_mem_db, const char *text_download_tbl);

        std::string replacePlaceholders(const std::string &query, const std::vector<std::string> &params);

        uint32_t extractBigEndian4Bytes(const unsigned char *data, size_t &start);

        uint16_t extractBigEndian2Bytes(const unsigned char *data, size_t &start);

        void
        processParseMessage(size_t &index, const size_t &bytes_transferred, std::queue<unsigned char> &clientQueue, std::queue<transaction::Query> &queryQueue) {
            size_t first_index = index;
            uint32_t message_size = extractBigEndian4Bytes(downstream_data_, index);

            std::string statement_id = extractString(downstream_data_, index);

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
                processBindMessage(index, bytes_transferred, clientQueue, query, queryQueue);
            } else {
                index = 0;
            }
        };


        // <パラメータの総数(2byte)><パラメータの型(2byte)><パラメータの総数(2byte)><<パラメータサイズ(4byte),データ(パラメータサイズ)>>
        // 呼び出す前にメッセージ形式の最初は処理済みであること('B'の次をindexが指していることがのぞましい)
        void processBindMessage(size_t &index, const size_t &bytes_transferred, std::queue<unsigned char> &clientQueue,
                                std::string query, std::queue<transaction::Query> &queryQueue) {
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

            // キューにクエリを追加
            queryQueue.push(transaction::Query(query));

            index = first_index + message_size; // 1 + message_size + 1
            std::cout << "next message bind: " << downstream_data_[index] << std::endl;

            if (downstream_data_[index] == 'D') {
                clientQueue.push('D');
                index++;
                processDescribeMessage(index, bytes_transferred, clientQueue, query, queryQueue);
            } else if (downstream_data_[index] == 'E') {
                clientQueue.push('E');
                index++;
                processExecuteMessage(index, bytes_transferred, clientQueue, query, queryQueue);
            }
        }

        void
        processDescribeMessage(size_t &index, const size_t &bytes_transferred, std::queue<unsigned char> &clientQueue,
                               std::string &query, std::queue<transaction::Query> &queries) {
            std::cout << "processDesc" << std::endl;
            uint32_t message_size = extractBigEndian4Bytes(downstream_data_, index);

            unsigned char ps_or_s = downstream_data_[index];
            index++;

            std::string ps_or_portal_name = extractString(downstream_data_, index);

            if (downstream_data_[index] == 'E') {
                clientQueue.push('E');
                processExecuteMessage(index, bytes_transferred, clientQueue, query, queries);
            }
        }

        void
        processExecuteMessage(size_t &index, const size_t &bytes_transferred, std::queue<unsigned char> &clientQueue,
                              std::string &query, std::queue<transaction::Query> &queries) {
            uint32_t message_size = extractBigEndian4Bytes(downstream_data_, index);

            std::string portal_name = extractString(downstream_data_, index);

            uint16_t return_row_size = extractBigEndian4Bytes(downstream_data_, index);

            std::cout << "next message execute: " << downstream_data_[index] << std::endl;
            if (downstream_data_[index] == 'S') {
                clientQueue.push('S');
                index++;
                processSyncMessage(index, bytes_transferred, clientQueue, query, queries);
            } else if (downstream_data_[index] == 'P') {
                clientQueue.push('P');
                index++;
                processParseMessage(index, bytes_transferred, clientQueue, queries);
            }
        };

        void processSyncMessage(size_t &index, const size_t &bytes_transferred, std::queue<unsigned char> &clientQueue,
                                std::string &query, std::queue<transaction::Query> &queries);

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
        sqlite3 *in_mem_db;
        std::map<std::string, std::string> prepared_statements_lists;
        static constexpr char *write_ahead_log = "/home/y-watanabe/wal.csv";
        static constexpr char *text_create_tbl_bench = "create table if not exists bench (pk text primary key, field1 integer, field2 integer, field3 integer)";
        static constexpr char *text_create_tbl_sbtest1 = "create table if not exists sbtest1 (id integer primary key, k integer, c text, pad text)";
        static constexpr char *text_download_sbtest1 = "select * from sbtest1";

        static constexpr char *text_create_OORDER = "CREATE TABLE OORDER ( \n"
                                                    "o_w_id int NOT NULL,\n"
                                                    "o_d_id int NOT NULL,\n"
                                                    "o_id int NOT NULL,\n"
                                                    "o_c_id int NOT NULL,\n"
                                                    "o_carrier_id int DEFAULT NULL,\n"
                                                    "o_ol_cnt decimal(2,0) NOT NULL,\n"
                                                    "o_all_local decimal(1,0) NOT NULL,\n"
                                                    "o_entry_d timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                                                    " PRIMARY KEY ((o_w_id,o_d_id) HASH,o_id)\n"
                                                    ")";
        static constexpr char *text_create_DISTRICT = "CREATE TABLE DISTRICT ( \n"
                                                      "d_w_id int NOT NULL,\n"
                                                      "d_id int NOT NULL,\n"
                                                      "d_ytd decimal(12,2) NOT NULL,\n"
                                                      "d_tax decimal(4,4) NOT NULL,\n"
                                                      "d_next_o_id int NOT NULL,\n"
                                                      "d_name varchar(10) NOT NULL,\n"
                                                      "d_street_1 varchar(20) NOT NULL,\n"
                                                      "d_street_2 varchar(20) NOT NULL,\n"
                                                      "d_city varchar(20) NOT NULL,\n"
                                                      "d_state char(2) NOT NULL,\n"
                                                      "d_zip char(9) NOT NULL,\n"
                                                      " PRIMARY KEY ((d_w_id,d_id) HASH)\n"
                                                      ")";
        static constexpr char *text_create_ITEM = "CREATE TABLE ITEM ( \n"
                                                  "i_id int NOT NULL,\n"
                                                  "i_name varchar(24) NOT NULL,\n"
                                                  "i_price decimal(5,2) NOT NULL,\n"
                                                  "i_data varchar(50) NOT NULL,\n"
                                                  "i_im_id int NOT NULL,\n"
                                                  " PRIMARY KEY (i_id)\n"
                                                  ")";
        static constexpr char *text_create_WAREHOUSE = "CREATE TABLE WAREHOUSE ( \n"
                                                       "w_id int NOT NULL,\n"
                                                       "w_ytd decimal(12,2) NOT NULL,\n"
                                                       "w_tax decimal(4,4) NOT NULL,\n"
                                                       "w_name varchar(10) NOT NULL,\n"
                                                       "w_street_1 varchar(20) NOT NULL,\n"
                                                       "w_street_2 varchar(20) NOT NULL,\n"
                                                       "w_city varchar(20) NOT NULL,\n"
                                                       "w_state char(2) NOT NULL,\n"
                                                       "w_zip char(9) NOT NULL,\n"
                                                       " PRIMARY KEY (w_id)\n"
                                                       ")";
        static constexpr char *text_create_CUSTOMER = "CREATE TABLE CUSTOMER ( \n"
                                                      "c_w_id int NOT NULL,\n"
                                                      "c_d_id int NOT NULL,\n"
                                                      "c_id int NOT NULL,\n"
                                                      "c_discount decimal(4,4) NOT NULL,\n"
                                                      "c_credit char(2) NOT NULL,\n"
                                                      "c_last varchar(16) NOT NULL,\n"
                                                      "c_first varchar(16) NOT NULL,\n"
                                                      "c_credit_lim decimal(12,2) NOT NULL,\n"
                                                      "c_balance decimal(12,2) NOT NULL,\n"
                                                      "c_ytd_payment float NOT NULL,\n"
                                                      "c_payment_cnt int NOT NULL,\n"
                                                      "c_delivery_cnt int NOT NULL,\n"
                                                      "c_street_1 varchar(20) NOT NULL,\n"
                                                      "c_street_2 varchar(20) NOT NULL,\n"
                                                      "c_city varchar(20) NOT NULL,\n"
                                                      "c_state char(2) NOT NULL,\n"
                                                      "c_zip char(9) NOT NULL,\n"
                                                      "c_phone char(16) NOT NULL,\n"
                                                      "c_since timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                                                      "c_middle char(2) NOT NULL,\n"
                                                      "c_data varchar(500) NOT NULL,\n"
                                                      " PRIMARY KEY ((c_w_id,c_d_id) HASH,c_id)\n"
                                                      ")";
        static constexpr char *text_create_ORDER_LINE = "CREATE TABLE ORDER_LINE ( \n"
                                                        "ol_w_id int NOT NULL,\n"
                                                        "ol_d_id int NOT NULL,\n"
                                                        "ol_o_id int NOT NULL,\n"
                                                        "ol_number int NOT NULL,\n"
                                                        "ol_i_id int NOT NULL,\n"
                                                        "ol_delivery_d timestamp NULL DEFAULT NULL,\n"
                                                        "ol_amount decimal(6,2) NOT NULL,\n"
                                                        "ol_supply_w_id int NOT NULL,\n"
                                                        "ol_quantity decimal(2,0) NOT NULL,\n"
                                                        "ol_dist_info char(24) NOT NULL,\n"
                                                        " PRIMARY KEY ((ol_w_id,ol_d_id) HASH,ol_o_id,ol_number)\n"
                                                        ")";
        static constexpr char *text_create_NEW_ORDER = "CREATE TABLE NEW_ORDER ( \n"
                                                       "no_w_id int NOT NULL,\n"
                                                       "no_d_id int NOT NULL,\n"
                                                       "no_o_id int NOT NULL,\n"
                                                       " PRIMARY KEY ((no_w_id,no_d_id) HASH,no_o_id)\n"
                                                       ")";
        static constexpr char *text_create_STOCK = "CREATE TABLE STOCK ( \n"
                                                   "s_w_id int NOT NULL,\n"
                                                   "s_i_id int NOT NULL,\n"
                                                   "s_quantity decimal(4,0) NOT NULL,\n"
                                                   "s_ytd decimal(8,2) NOT NULL,\n"
                                                   "s_order_cnt int NOT NULL,\n"
                                                   "s_remote_cnt int NOT NULL,\n"
                                                   "s_data varchar(50) NOT NULL,\n"
                                                   "s_dist_01 char(24) NOT NULL,\n"
                                                   "s_dist_02 char(24) NOT NULL,\n"
                                                   "s_dist_03 char(24) NOT NULL,\n"
                                                   "s_dist_04 char(24) NOT NULL,\n"
                                                   "s_dist_05 char(24) NOT NULL,\n"
                                                   "s_dist_06 char(24) NOT NULL,\n"
                                                   "s_dist_07 char(24) NOT NULL,\n"
                                                   "s_dist_08 char(24) NOT NULL,\n"
                                                   "s_dist_09 char(24) NOT NULL,\n"
                                                   "s_dist_10 char(24) NOT NULL,\n"
                                                   " PRIMARY KEY (s_w_id HASH, s_i_id ASC)\n"
                                                   ")";
        static constexpr char *text_create_HISTORY = "CREATE TABLE HISTORY ( \n"
                                                     "h_c_id int NOT NULL,\n"
                                                     "h_c_d_id int NOT NULL,\n"
                                                     "h_c_w_id int NOT NULL,\n"
                                                     "h_d_id int NOT NULL,\n"
                                                     "h_w_id int NOT NULL,\n"
                                                     "h_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                                                     "h_amount decimal(6,2) NOT NULL,\n"
                                                     "h_data varchar(24) NOT NULL\n"
                                                     ")";

        // inner class
    public:
        class acceptor final {
        public:
            explicit acceptor(boost::asio::io_service &io_service,
                              const std::string &local_host, unsigned short local_port,
                              const std::string &upstream_host, unsigned short upstream_port);

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
        };
    };
}