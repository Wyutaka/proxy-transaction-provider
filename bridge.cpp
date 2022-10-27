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
#include "./src/reqestresponse/Constants.h++"
#include "src/connector/kvs/slow_postgres.h++"

#define CASS_SHARED_PTR(type, v)                                                                   \
    std::shared_ptr<std::remove_pointer_t<decltype(v)>>(v, [](decltype(v) t) {                     \
        if (t)                                                                                     \
            cass_##type##_free(t);                                                                 \
    })

namespace tcp_proxy {
    typedef ip::tcp::socket socket_type;
    typedef boost::shared_ptr<bridge> ptr_type;
    bridge::bridge(boost::asio::io_service& ios, unsigned short upstream_port
            , std::string upstream_host): downstream_socket_(ios),
            upstream_socket_(ios),
            upstream_host_(upstream_host_),
            upstream_port_(upstream_port_),
            _connectFuture(NULL),
            _cluster(CASS_SHARED_PTR(cluster, cass_cluster_new())),
            _session(std::shared_ptr<CassSession>(cass_session_new(), transaction::detail::SessionDeleter()))
            {
                // cassandraのコネクション
                // TODO コネクション、keyspaceは決め打ち
                cass_cluster_set_contact_points(_cluster.get(), backend_host);
                cass_cluster_set_protocol_version(_cluster.get(), CASS_PROTOCOL_VERSION_V4);
                _connectFuture =
                        CASS_SHARED_PTR(future, cass_session_connect_keyspace_n(_session.get(), _cluster.get(), "txbench", 7)); //7-> keyspace_length;

                if (cass_future_error_code(_connectFuture.get()) != CASS_OK) {
                    std::cerr << "cannot connect to Cassandra" << std::endl;
                    std::terminate();
                }
            }

    bridge::~bridge() {
        _connectFuture.reset();
        _session.reset();
        _cluster.reset();
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
//            debug::hexdump(reinterpret_cast<const char *>(downstream_data_), bytes_transferred); // 下流バッファバッファ16進表示

            // ヘッダ情報の読み込み
            int n = 0;

            if(downstream_data_[0] == 0x04 && downstream_data_[4] == cassprotocol::opcode::QUERY) { //is cassandra request version
//                std::cout << "cql detected" << std::endl;
                n = (int)downstream_data_[5] << 24;
                n += (int)downstream_data_[6] << 16;
                n += (int)downstream_data_[7] << 8;
                n += (int)downstream_data_[8];
//                std::cout << "one query size" << n << std::endl;
                if (n != 0) {

                    transaction::lock::Lock<transaction::TransactionProviderImpl<transaction::SlowCassandraConnector>> lock{transaction::TransactionProviderImpl<transaction::SlowCassandraConnector>(transaction::SlowCassandraConnector("127.0.0.1", shared_from_this()))};
                    // 13~bodyまで切り取り
                    const transaction::Request& req = transaction::Request(transaction::Peer(upstream_host_, upstream_port_), std::string(reinterpret_cast<const char *>(&downstream_data_[13]), n), std::string(reinterpret_cast<const char *>(&downstream_data_), bytes_transferred));

                    // レイヤー移動
                    const auto& res = lock(req);

                    if (res.begin()->status() == transaction::Status::Ok) {// レスポンスでOKが帰って来たとき(begin,commitを想定)
                        std::cout << "status ok" << std::endl;
                        // クエリに対するリクエストを返す
                        // ヘッダ + レスポンスボディ
                        // 原型生成
                        unsigned char result_ok[13] =  {0x84, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x04,0x00, 0x00, 0x00, 0x01};
//                        memcpy(&result_ok[2], &downstream_data_[2], 2);
                        result_ok[2] = downstream_data_[2];         // streamIDの置換
                        result_ok[3] = downstream_data_[3];         // streamIDの置換
//                        debug::hexdump(result_ok, 4);
//                        std::cout << result_ok[0] << std::endl;
                        // streamIdコピー
                        async_write(downstream_socket_,
                                    boost::asio::buffer(result_ok,13), // result_okの文字列長
                                    boost::bind(&bridge::handle_downstream_write,
                                                shared_from_this(),
                                                boost::asio::placeholders::error));

                        // Setup async read from client (downstream)
                        downstream_socket_.async_read_some(
                                boost::asio::buffer(downstream_data_,max_data_length),
                                boost::bind(&bridge::handle_downstream_read,
                                            shared_from_this(),
                                            boost::asio::placeholders::error,
                                            boost::asio::placeholders::bytes_transferred));
                    }
                }
                // TODO postgresの対応
            } else if(downstream_data_[0] == 0x51) { // 1バイト目が'Q'のとき

//                std::cout << "postgres" << std::endl;

                // Body部の計算 -> 2,3,4,5バイト目でクエリ全体のサイズ計算 -> クエリの種類と長さの情報を切り取る
                n = (int)downstream_data_[1] << 24;
                n += (int)downstream_data_[2] << 16;
                n += (int)downstream_data_[3] << 8;
                n += (int)downstream_data_[4];

                // lock層の生成(postgres用)
                transaction::lock::Lock<transaction::TransactionProviderImpl<transaction::CassandraConnectorPGSQL>> lock{transaction::TransactionProviderImpl<transaction::CassandraConnectorPGSQL>(transaction::CassandraConnectorPGSQL(shared_from_this(), _cluster, _session))};
                // リクエストの生成
                const transaction::Request& req = transaction::Request(transaction::Peer(upstream_host_, upstream_port_), std::string(reinterpret_cast<const char *>(&downstream_data_[5]), n - 4), std::string(reinterpret_cast<const char *>(&downstream_data_), bytes_transferred)); // n-4 00まで含める｀h
                // レスポンス生成
                const auto& res = lock(req);

                /* TODO クエリに対するリクエストを返す
                     クライアントに返す文字列の形式: <C/Z
                     Byte1('C') |  Int32       |  String    |  Byte1('Z')  |  Int32(5)   |  Byte1
                     C          |   Int32      |  hoge      |      Z       | 00 00 00 05 |  {'I'|'T'|'E'}  ('I'-> not in transaction/'T'-> in transaction/'E'-> 'Error transaction')
                     43         |  xx xx xx xx | xx xx ...  |      5a      | 00 00 00 05 |  {49/54/45}
                */

                unsigned char res_ok[16 + 6] = {0x43, 0x00, 0x00, 0x00, 0x0f, 0x49, 0x4e, 0x53, 0x45, 0x52, 0x54, 0x20, 0x30, 0x20, 0x31, 0x00, // C
                                                    0x5a, 0x00, 0x00, 0x00, 0x05, 0x54 // Z
                                               };

                // TODO 失敗、成功をレスポンスから判定して返す
                // クライアントにレスポンスを返す
                async_write(downstream_socket_,
                            boost::asio::buffer(res_ok,16+6), // result_okの文字列長
                            boost::bind(&bridge::handle_downstream_write,
                                        shared_from_this(),
                                        boost::asio::placeholders::error));

                // クライアントからの読み込みを開始 (downstream)
                downstream_socket_.async_read_some(
                        boost::asio::buffer(downstream_data_,max_data_length),
                        boost::bind(&bridge::handle_downstream_read,
                                    shared_from_this(),
                                    boost::asio::placeholders::error,
                                    boost::asio::placeholders::bytes_transferred));


//                async_write(upstream_socket_,
//                            boost::asio::buffer(downstream_data_,bytes_transferred),
//                            boost::bind(&bridge::handle_upstream_write,
//                                        shared_from_this(),
//                                        boost::asio::placeholders::error));

            } else {
                async_write(upstream_socket_,
                            boost::asio::buffer(downstream_data_,bytes_transferred),
                            boost::bind(&bridge::handle_upstream_write,
                                        shared_from_this(),
                                        boost::asio::placeholders::error));
            }

        }
        else {
            close();
        }
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
