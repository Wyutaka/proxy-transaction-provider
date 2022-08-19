//
// Created by mint25mt on 2022/08/19.
//

#include <cstdlib>
#include <cstddef>
#include <iostream>
#include <string>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include "server.h++"

namespace tcp_proxy {
    bridge::acceptor::acceptor(boost::asio::io_service& io_service,
    const std::string& local_host, unsigned short local_port,
    const std::string& upstream_host, unsigned short upstream_port)
    : io_service_(io_service),
    localhost_address(boost::asio::ip::address_v4::from_string(local_host)),
    acceptor_(io_service_,ip::tcp::endpoint(localhost_address,local_port)),
    upstream_port_(upstream_port),
    upstream_host_(upstream_host)
    {}

    bridge::acceptor::~acceptor() {

    }

    bool bridge::acceptor::accept_connections()
    {

        try
        {
            session_ = boost::shared_ptr<bridge>(new bridge(io_service_));

            acceptor_.async_accept(session_->downstream_socket(),
                                   boost::bind(&acceptor::handle_accept,
                                               this,
                                               boost::asio::placeholders::error));
        }
        catch(std::exception& e)
        {
            std::cerr << "acceptor exception: " << e.what() << std::endl;
            return false;
        }

        return true;
    }

    void bridge::acceptor::handle_accept(const boost::system::error_code& error)
    {
        if (!error)
        {
            session_->start(upstream_host_,upstream_port_);

            if (!accept_connections())
            {
                std::cerr << "Failure during call to accept." << std::endl;
            }
        }
        else
        {
            std::cerr << "Error: " << error.message() << std::endl;
        }
    }
}
