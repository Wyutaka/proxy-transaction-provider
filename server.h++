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
        socket_type upstream_socket_;

    private:
        void handle_upstream_read(const boost::system::error_code& error, const size_t& bytes_transferred);
        void handle_downstream_write(const boost::system::error_code& error);
        socket_type downstream_socket_;

        enum { max_data_length = 8192 * 2 }; //16KB
        unsigned char downstream_data_[max_data_length];
        unsigned char upstream_data_  [max_data_length];
        boost::mutex mutex_;
        static constexpr char* backend_host = "192.168.12.23,192.168.12.22,192.168.12.21,192.168.12.20,192.168.12.19,192.168.12.18,192.168.12.17,192.168.12.16,192.168.12.15,192.168.12.14";
        unsigned short upstream_port_;
        std::string upstream_host_;
        std::shared_ptr<CassFuture> _connectFuture;
        std::shared_ptr<CassCluster> _cluster;
        std::shared_ptr<CassSession> _session;

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