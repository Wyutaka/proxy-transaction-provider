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
#include "src/ThreadPool/ThreadPool.h++"
//#include "src/reqestresponse/Request.h++"
#include <sqlite3.h>


namespace tcp_proxy {
    namespace ip = boost::asio::ip;
    class bridge : public boost::enable_shared_from_this<bridge> {
        typedef ip::tcp::socket socket_type;
        typedef boost::shared_ptr<bridge> ptr_type;
    public:
        explicit bridge(boost::asio::io_service& ios, unsigned short upstream_port
        , std::string upstream_host);
        ~bridge();
        socket_type& downstream_socket();
        socket_type& upstream_socket();
        void start(const std::string& upstream_host, unsigned short upstream_port);
        void close();
        void handle_upstream_connect(const boost::system::error_code& error);
        /*
            Section B: Client --> Proxy --> Remove Server
            Process data recieved from client then write to remove server.
        */

        // Read from client complete, now send data to remote server
        void handle_downstream_read(const boost::system::error_code& error,
                                    const size_t& bytes_transferred);
        // Write to remote server complete, Async read from client
        void handle_upstream_write(const boost::system::error_code& error);
        static void send_queue_backend(std::queue<std::string>& queue);
        socket_type upstream_socket_;

    private:
        void handle_upstream_read(const boost::system::error_code& error, const size_t& bytes_transferred);
        void handle_downstream_write(const boost::system::error_code& error);
        void handle_downstream_write_proxy(const boost::system::error_code& error);
//        void download_result(PGconn &conn, const transaction::Request &req, sqlite3 *in_mem_db);

        socket_type downstream_socket_;

        enum { max_data_length = 8192 * 2 }; //16KB
        unsigned char downstream_data_[max_data_length];
        unsigned char upstream_data_  [max_data_length];
        boost::mutex mutex_;
        static constexpr char* backend_host = "192.168.12.23";
        static constexpr char*  backend_postgres_conninfo = "host=192.168.12.23 port=5433 dbname=yugabyte user=yugabyte password=yugabyte";
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
        static constexpr char* write_ahead_log = "/home/y-watanabe/wal.csv";
        static constexpr char* text_create_tbl_bench = "create table if not exists bench (pk text primary key, field1 integer, field2 integer, field3 integer)";
        static constexpr char* text_create_tbl_sbtest1 = "create table if not exists sbtest1 (id integer primary key, k integer, c text, pad text)";
        static constexpr char* text_download_sbtest1 = "select * from sbtest1";


        // inner class
    public:
        class acceptor final
        {
        public:
            explicit acceptor(boost::asio::io_service& io_service,
                     const std::string& local_host, unsigned short local_port,
                     const std::string& upstream_host, unsigned short upstream_port);
            ~acceptor();
            bool accept_connections();
        private:
            void handle_accept(const boost::system::error_code& error);

            boost::asio::io_service& io_service_;
            ip::address_v4 localhost_address;
            ip::tcp::acceptor acceptor_;
            ptr_type session_;
            unsigned short upstream_port_;
            std::string upstream_host_;
        };
    };
}