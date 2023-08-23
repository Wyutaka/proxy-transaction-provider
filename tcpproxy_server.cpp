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

//        std::cout << "test1" << std::endl;
        acceptor.accept_connections();
//        std::cout << "test2" << std::endl;

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

//    std::string forward_hosts[10] = {"192.168.12.23", "192.168.12.22", "192.168.12.21", "192.168.12.20", "192.168.12.19", "192.168.12.18", "192.168.12.17", "192.168.12.16", "192.168.12.15","192.168.12.14"};
//    unsigned short local_ports[10] = {5432, 5433, 5434, 5435, 5436, 5437, 5438, 5439, 5440, 5441};

    std::string forward_hosts[10] = {"192.168.12.17"};
    unsigned short local_ports[10] = {5432};

    boost::thread_group threads;
//    for (short i = 0; i < 16; i++) {
//        threads.create_thread([local_ports, forward_port, local_host, forward_hosts, i] { return run_proxy(local_ports[0] + i, forward_port, local_host, forward_hosts[i % 10]); });
//        local_port++;
//    }
    threads.create_thread([local_ports, forward_port, local_host, forward_hosts] { return run_proxy(local_ports[0], forward_port, local_host, forward_hosts[0]); });

    threads.join_all();

   return 0;
}

