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
#include "./src/lock/Lock.h++"
#include "./src/transaction/transaction_impl.hpp"
#include "./src/test/DumpHex.h++"
#include "./src/connector/kvs/cassprotocol.h++"

namespace tcp_proxy {
    typedef ip::tcp::socket socket_type;
    typedef boost::shared_ptr<bridge> ptr_type;
    bridge::bridge(boost::asio::io_service& ios, unsigned short upstream_port
            , std::string upstream_host): downstream_socket_(ios),
            upstream_socket_(ios),
            upstream_host_(upstream_host_),
            upstream_port_(upstream_port_) {}

    bridge::~bridge() {

    }
    socket_type& bridge::downstream_socket()
    {
        // Client socket
        return downstream_socket_;
    }

    socket_type& bridge::upstream_socket()
    {
        // Remote server socket
        return upstream_socket_;
    }

    void bridge::start(const std::string& upstream_host, unsigned short upstream_port)
    {
        // Attempt connection to remote server (upstream side)
        upstream_socket_.async_connect(
                ip::tcp::endpoint(
                        boost::asio::ip::address::from_string(upstream_host),
                        upstream_port),
                boost::bind(&bridge::handle_upstream_connect,
                            shared_from_this(),
                            boost::asio::placeholders::error));
    }

    void bridge::handle_upstream_connect(const boost::system::error_code& error)
    {
        if (!error)
        {
            // Setup async read from remote server (upstream)
            upstream_socket_.async_read_some(
                    boost::asio::buffer(upstream_data_,max_data_length),
                    boost::bind(&bridge::handle_upstream_read,
                                shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));

            // Setup async read from client (downstream)
            downstream_socket_.async_read_some(
                    boost::asio::buffer(downstream_data_,max_data_length),
                    boost::bind(&bridge::handle_downstream_read,
                                shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));
        }
        else
            close();
    }

    void bridge::handle_upstream_read(const boost::system::error_code& error,
                              const size_t& bytes_transferred)
    {
        if (!error)
        {
            int n;
            if(upstream_data_[0] == 0x84) { //is cassandra response version
                n = (int)upstream_data_[5] << 24;
                n += (int)upstream_data_[6] << 16;
                n += (int)upstream_data_[7] << 8;
                n += (int)upstream_data_[8];

            } else {
                n = bytes_transferred;
            }

//            std::cout << "handle upstream_read" << std::endl;
//            debug::hexdump(reinterpret_cast<const char *>(upstream_data_), bytes_transferred);

            async_write(downstream_socket_,
                        boost::asio::buffer(upstream_data_,bytes_transferred),
                        boost::bind(&bridge::handle_downstream_write,
                                    shared_from_this(),
                                    boost::asio::placeholders::error));
        }
        else
            close();
    }

    void bridge::handle_downstream_write(const boost::system::error_code& error)
    {
        if (!error)
        {
            upstream_socket_.async_read_some(
                    boost::asio::buffer(upstream_data_,max_data_length),
                    boost::bind(&bridge::handle_upstream_read,
                                shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));
        }
        else
            close();
    }

    template <class C>
    void print(const C& c, std::ostream& os = std::cout)
    {
        std::for_each(std::begin(c), std::end(c), [&os](typename C::value_type p) { os << '{' << p.first << ',' << &p.second << "}, "; });
        os << std::endl;
    }

    void bridge::handle_downstream_read(const boost::system::error_code& error,
                                const size_t& bytes_transferred)
    {
        if (!error)
        {
//            std::cout << "handle downstream_read" << std::endl;
//            debug::hexdump(reinterpret_cast<const char *>(downstream_data_), 200);

            // ヘッダ情報の読み込み
            int n;

            std::cout << "downstream_data_size" << bytes_transferred << std::endl;
            if(downstream_data_[0] == 0x04 && downstream_data_[4] == cassprotocol::opcode::QUERY) { //is cassandra request version
//                std::cout << "cql detected" << std::endl;
                n = (int)downstream_data_[5] << 24;
                n += (int)downstream_data_[6] << 16;
                n += (int)downstream_data_[7] << 8;
                n += (int)downstream_data_[8];
                std::cout << "one query size" << n << std::endl;
                if (n != 0) {

                    transaction::lock::Lock<transaction::TransactionProviderImpl<transaction::SlowCassandraConnector>> lock{transaction::TransactionProviderImpl<transaction::SlowCassandraConnector>(transaction::SlowCassandraConnector("127.0.0.1", shared_from_this()))};
                    // 13~bodyまで切り取り
                    const transaction::Request& req = transaction::Request(transaction::Peer(upstream_host_, upstream_port_), std::string(reinterpret_cast<const char *>(&downstream_data_[13]), n), std::string(reinterpret_cast<const char *>(&downstream_data_), bytes_transferred));

                    // レイヤー移動
                    const auto &res = lock(req);

//                    std::cout << "downstream_data" << std::endl;
//                    debug::hexdump(reinterpret_cast<const char *>(downstream_data_), bytes_transferred);
//                    std::cout << "raw_request" << std::endl;
//                    debug::hexdump(req.raw_request().data(), req.raw_request().size());

//                    async_write(upstream_socket_,
//                                boost::asio::buffer(req.raw_request().data(),req.raw_request().size()),
//                                boost::bind(&bridge::handle_upstream_write,
//                                            shared_from_this(),
//                                            boost::asio::placeholders::error));

                }
                // TODO 再接続要求が返されないかも

            } else {
                async_write(upstream_socket_,
                            boost::asio::buffer(downstream_data_,bytes_transferred),
                            boost::bind(&bridge::handle_upstream_write,
                                        shared_from_this(),
                                        boost::asio::placeholders::error));
            }

        }
        else {
            std::cout << "handle downstream_read errr" << std::endl; // <- misc:2
            std::cout << error <<std::endl;
            close();
        }
    }


    void bridge::handle_upstream_write(const boost::system::error_code& error)
    {
        if (!error)
        {
            std::cout << "handle upstream write" << std::endl;
            downstream_socket_.async_read_some(
                    boost::asio::buffer(downstream_data_,max_data_length),
                    boost::bind(&bridge::handle_downstream_read,
                                shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));
        }
        else
            close();
    }

    void bridge::close()
    {
        boost::mutex::scoped_lock lock(mutex_);

        if (downstream_socket_.is_open())
        {
            downstream_socket_.close();
        }

        if (upstream_socket_.is_open())
        {
            upstream_socket_.close();
        }
    }

}// namespace tcp_proxy
