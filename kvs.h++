//
// Created by mint25mt on 2022/08/23.
//

#ifndef MY_PROXY_KVS_H
#define MY_PROXY_KVS_H

#include <string>
#include <vector>

#include "src/reqestresponse/Request.h++"
#include "src/reqestresponse/CoResponse.h++"

namespace transaction {

    namespace kvs {
        class Connector {
        public:
            virtual ~Connector() = default;

        public:
            virtual Response process(const Request &) = 0;

        public:
            Response operator()(const Request &request) { return process(request); }
        };
    } // namespace kvs
} // namespace transaction

#endif //MY_PROXY_KVS_H
