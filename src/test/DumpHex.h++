#pragma once

#include <ctype.h>
#include <stdio.h>
#include <string_view>


namespace debug {
    void hexdump(const char *ptr, unsigned int buflen);

}