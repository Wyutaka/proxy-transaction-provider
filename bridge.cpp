//
// Created by mint25mt on 2022/08/19.
//

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include "server.h++"
#include <iostream>
#include "./src/lock/Lock.h++"
#include "./src/transaction/parallelizable_sqlite3_wal.hpp"
#include "./src/connector/kvs/slow.hpp"

namespace tcp_proxy {
    typedef ip::tcp::socket socket_type;
    typedef boost::shared_ptr<bridge> ptr_type;
    bridge::bridge(boost::asio::io_service& ios): downstream_socket_(ios),
                                                  upstream_socket_(ios)
    {}

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

    void bridge::handle_downstream_read(const boost::system::error_code& error,
                                const size_t& bytes_transferred)
    {
        if (!error)
        {
            for (int i = 0; i < max_data_length; ++i) {
                std::cout << downstream_data_[i];
            }
            // TODO ここからLock相に移動
            transaction::lock::Lock<transaction::detail::TransactionProviderImpl<transaction::SlowCassandraConnector>> lock{transaction::detail::TransactionProviderImpl<transaction::SlowCassandraConnector>{transaction::SlowCassandraConnector("localhost")}};

            std::cout << std::endl;
            async_write(upstream_socket_,
                        boost::asio::buffer(downstream_data_,bytes_transferred),
                        boost::bind(&bridge::handle_upstream_write,
                                    shared_from_this(),
                                    boost::asio::placeholders::error));
        }
        else
            close();
    }

    void bridge::handle_upstream_write(const boost::system::error_code& error)
    {
        if (!error)
        {
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
