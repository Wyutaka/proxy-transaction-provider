//
// Created by user1 on 22/09/29.
//

#ifndef MY_PROXY_CONSTANTS_H
#define MY_PROXY_CONSTANTS_H

namespace response::constants {
    constexpr char ok[] = {0x00, 0x00, 0x00, 0x01};

    /*
    0         8        16        24        32         40
    +---------+---------+---------+---------+---------+
    | version |  flags  |      stream       | opcode  |
    +---------+---------+---------+---------+---------+
    |                length                 |
    +---------+---------+---------+---------+
    |                                       |
    .            ...  body ...              .
    .                                       .
    .                                       .
    +----------------------------------------
    */

    constexpr char result_ok[] = {static_cast<char>(0x84), 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x04,0x00, 0x00, 0x00, 0x01};
};

#endif //MY_PROXY_CONSTANTS_H
