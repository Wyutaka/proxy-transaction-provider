//
// Created by user1 on 22/09/05.
//

#include "DumpHex.h++"
#include <ctype.h>
#include <stdio.h>
#include <string_view>

void hexdump(void *ptr, int buflen) {
    auto *buf = (unsigned char*)ptr;
    int i, j;
    for (i=0; i<buflen; i+=16) {
        printf("%06x: ", i);
        for (j=0; j<16; j++)
            if (i+j < buflen)
                printf("%02x ", buf[i+j]);
            else
                printf("   ");
        printf(" ");
        for (j=0; j<16; j++)
            if (i+j < buflen)
                printf("%c", isprint(buf[i+j]) ? buf[i+j] : '.');
        printf("\n");
    }
}

int main() {

    std::string_view sv = "stable diffusion stable diffusion stable +@(RQ%^*(&(";
    hexdump((void *) sv.data(), sv.size());

    return 0;
}

