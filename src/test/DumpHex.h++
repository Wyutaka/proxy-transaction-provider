#pragma once

#include <ctype.h>
#include <stdio.h>
#include <string_view>


namespace debug {
    void hexdump(const char *ptr, unsigned int buflen) {
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

}