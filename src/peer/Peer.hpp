//
// Created by mint25mt on 2022/08/23.
//

#ifndef MY_PROXY_PEER_HPP
#define MY_PROXY_PEER_HPP


#ifndef TRANSACTION_REQRES_HPP
#define TRANSACTION_REQRES_HPP

#include <string>
#include <unordered_map>
#include <vector>
#include <string_view>

#include "query.h++"
#include "src/reqestresponse/RowData.h++"
#include "src/reqestresponse/CoResponse.h++"
#include <boost/algorithm/string/replace.hpp>
#include <any>

namespace transaction {
    template <class IpAddress> class BasicPeer {
    public:
        BasicPeer();

        BasicPeer(IpAddress ip, std::uint16_t port);

        bool operator<(const BasicPeer &rhs) const;

        [[nodiscard]] const IpAddress &ip() const;
        [[nodiscard]] std::uint16_t port() const noexcept;

        bool operator==(const BasicPeer &peer) const;
        bool operator!=(const BasicPeer &peer) const;

    private:
        IpAddress _peerIp;
        std::uint16_t _peerPort;

    };

    template <class IpAddress> struct BasicPeerHash {
    private:
        std::hash<IpAddress> ip_hash_;
        std::hash<std::uint16_t> port_hash_;

    public:
        std::size_t operator()(const BasicPeer<IpAddress> &peer) const {
            return ip_hash_(peer.ip()) ^ port_hash_(peer.port());
        }
    };

} // namespace transaction

#endif // TRANSACTION_REQRES_HPP
#endif //MY_PROXY_PEER_HPP
