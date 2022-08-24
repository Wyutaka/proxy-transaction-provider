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
    private:
        IpAddress _peerIp;
        std::uint16_t _peerPort;

    public:
        BasicPeer() = default;

        BasicPeer(IpAddress ip, std::uint16_t port)
                : _peerIp(std::move(ip))
                , _peerPort(port) {}

    public:
        bool operator<(const BasicPeer &rhs) const {
            return _peerPort < rhs._peerPort || _peerIp < rhs._peerIp;
        }

    public:
        [[nodiscard]] const IpAddress &ip() const { return _peerIp; }
        [[nodiscard]] std::uint16_t port() const noexcept { return _peerPort; }

    public:
        bool operator==(const BasicPeer &peer) const {
            return peer._peerPort == _peerPort && peer._peerIp == _peerIp;
        }
        bool operator!=(const BasicPeer &peer) const { return !((*this) == peer); }
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
