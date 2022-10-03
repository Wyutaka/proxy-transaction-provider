#include <cstdlib>
#include <cstddef>
#include <iostream>
#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread.hpp>

#include "server.h++"

void run_proxy(unsigned short local_port, unsigned short forward_port
    , const std::string local_host, const std::string forward_host)
{

    boost::asio::io_service ios;
    try
    {
        tcp_proxy::bridge::acceptor acceptor(ios,
                                             local_host, local_port,
                                             forward_host, forward_port);

        acceptor.accept_connections();

        ios.run();
    }
    catch(std::exception& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

int main(int argc, char* argv[])
{
    if (argc != 5)
    {
        std::cerr << "usage: tcpproxy_server <local host ip> <local port> <forward host ip> <forward port>" << std::endl;
      return 1;
   }

   const int num_threads = 4;
   auto local_port   = static_cast<unsigned short>(::atoi(argv[2]));
   const auto forward_port = static_cast<unsigned short>(::atoi(argv[4]));
   const std::string local_host      = argv[1];
   const std::string forward_host    = argv[3];

    boost::thread_group threads;
    for (int i = 0; i < 1; i++) {
        threads.create_thread([local_port, forward_port, local_host, forward_host] { return run_proxy(local_port, forward_port, local_host, forward_host); });
        local_port++;
    }
    threads.join_all();

   return 0;
}

