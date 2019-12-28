/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.

Original Copyright:
See URL: https://github.com/greensky00/hex-dump
         (v0.1.8)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#pragma once

#include <inttypes.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

#ifndef _CLM_DEFINED
#define _CLM_DEFINED   (1)

#ifdef HEX_DUMP_NO_COLOR
    #define _CLM_D_GRAY     ""
    #define _CLM_GREEN      ""
    #define _CLM_B_GREEN    ""
    #define _CLM_RED        ""
    #define _CLM_B_RED      ""
    #define _CLM_BROWN      ""
    #define _CLM_B_BROWN    ""
    #define _CLM_BLUE       ""
    #define _CLM_B_BLUE     ""
    #define _CLM_MAGENTA    ""
    #define _CLM_B_MAGENTA  ""
    #define _CLM_CYAN       ""
    #define _CLM_END        ""

    #define _CLM_WHITE_FG_RED_BG    ""
#else
    #define _CLM_D_GRAY     "\033[1;30m"
    #define _CLM_GREEN      "\033[32m"
    #define _CLM_B_GREEN    "\033[1;32m"
    #define _CLM_RED        "\033[31m"
    #define _CLM_B_RED      "\033[1;31m"
    #define _CLM_BROWN      "\033[33m"
    #define _CLM_B_BROWN    "\033[1;33m"
    #define _CLM_BLUE       "\033[34m"
    #define _CLM_B_BLUE     "\033[1;34m"
    #define _CLM_MAGENTA    "\033[35m"
    #define _CLM_B_MAGENTA  "\033[1;35m"
    #define _CLM_CYAN       "\033[36m"
    #define _CLM_B_GREY     "\033[1;37m"
    #define _CLM_END        "\033[0m"

    #define _CLM_WHITE_FG_RED_BG    "\033[37;41m"
#endif

#define _CL_D_GRAY(str)     _CLM_D_GRAY     str _CLM_END
#define _CL_GREEN(str)      _CLM_GREEN      str _CLM_END
#define _CL_RED(str)        _CLM_RED        str _CLM_END
#define _CL_B_RED(str)      _CLM_B_RED      str _CLM_END
#define _CL_MAGENTA(str)    _CLM_MAGENTA    str _CLM_END
#define _CL_BROWN(str)      _CLM_BROWN      str _CLM_END
#define _CL_B_BROWN(str)    _CLM_B_BROWN    str _CLM_END
#define _CL_B_BLUE(str)     _CLM_B_BLUE     str _CLM_END
#define _CL_B_MAGENTA(str)  _CLM_B_MAGENTA  str _CLM_END
#define _CL_CYAN(str)       _CLM_CYAN       str _CLM_END
#define _CL_B_GRAY(str)     _CLM_B_GREY     str _CLM_END

#define _CL_WHITE_FG_RED_BG(str)    _CLM_WHITE_FG_RED_BG    str _CLM_END

#endif

// LCOV_EXCL_START

struct print_hex_options {
    // If set, print colorful hex dump using ANSI color codes.
    int enable_colors;
    // If set, print actual memory address.
    int actual_address;
    // The number of bytes per line.
    int align;
};

#define PRINT_HEX_OPTIONS_INITIALIZER \
    (struct print_hex_options){1, 1, 16}

static void _print_white_space(FILE* stream,
                               size_t len) {
    for (size_t i=0; i<len; ++i) {
        fprintf(stream, " ");
    }
}

static void __attribute__((unused))
            print_hex_stream(FILE* stream,
                             const void* buf,
                             size_t buflen,
                             struct print_hex_options options)
{
    size_t i, j;
    size_t max_addr_len;
    char str_buffer[256];

    // Check if buffer is empty.
    if (!buf || buflen == 0) {
        fprintf(stream, "%p, %zu\n", buf, buflen);
        return;
    }

    // Get the longest address string length
    if (options.actual_address) {
        sprintf(str_buffer, "%p", (char*)buf + buflen);
    } else {
        sprintf(str_buffer, "0x%zx", buflen);
    }

    max_addr_len = strlen(str_buffer);

    // Header (address, length)
    fprintf(stream,
            (options.enable_colors)
            ? _CL_B_RED("%p") " -- " _CL_B_RED("%p")
            : "%p -- %p",
            buf, (char*)buf + buflen - 1);
    fprintf(stream, ", %zu (0x%zx) bytes\n",
            buflen, buflen);

    // Legend
    _print_white_space(stream, max_addr_len);
    fprintf(stream, "   ");
    for (i = 0; i < (size_t)options.align; ++i) {
        if (options.align <= 16) {
            fprintf(stream,
                    (options.enable_colors)
                    ? _CL_CYAN(" %x ")
                    : " %x ",
                    (int)i);
        } else {
            fprintf(stream,
                    (options.enable_colors)
                    ? _CL_CYAN("%02x ")
                    : "%02x ",
                    (int)i);
        }

        if ((i + 1) % 8 == 0) {
            fprintf(stream, " ");
        }
    }
    fprintf(stream, "\n");

    size_t surplus_bytes = 0;
    uint64_t start_address = (uint64_t)buf;
    if (options.actual_address) {
        // In actual address mode, we do not start from the
        // leftmost spot as address might not be aligned.
        // In this case, calculate the 'surplus_bytes',
        // which denotes the number of bytes to be skipped.
        start_address /= options.align;
        start_address *= options.align;
        surplus_bytes = (uint64_t)buf - start_address;
    }

    for (i = 0; i < buflen + surplus_bytes; i += options.align) {
        // Address
        if (options.actual_address) {
            sprintf(str_buffer, "%p", (char*)start_address + i);
        } else {
            sprintf(str_buffer, "0x%x", (int)i);
        }
        _print_white_space(stream, max_addr_len - strlen(str_buffer));

        fprintf(stream,
                (options.enable_colors)
                ? _CL_CYAN("%s   ")
                : "%s   ",
                str_buffer);

        // Hex part
        for (j = i; j < i+options.align; ++j){
            if (j < buflen + surplus_bytes &&
                start_address + j >= (uint64_t)buf &&
                start_address + j <  (uint64_t)buf + buflen) {
                uint64_t idx = j - surplus_bytes;
                fprintf(stream,
                        (options.enable_colors)
                        ? _CL_GREEN("%02x ")
                        : "%02x ",
                        ((uint8_t*)buf)[idx]);
            } else {
                fprintf(stream, "   ");
            }

            if ((j + 1) % 8 == 0) {
                fprintf(stream, " ");
            }
        }

        // Ascii character part
        fprintf(stream, " ");
        for (j = i; j < i+options.align; ++j){
            if (j < buflen + surplus_bytes &&
                start_address + j >= (uint64_t)buf &&
                start_address + j <  (uint64_t)buf + buflen) {
                uint64_t idx = j - surplus_bytes;

                // print only readable ascii character
                if (0x20 <= ((char*)buf)[idx] && ((char*)buf)[idx] <= 0x7d) {
                    // Printable character
                    fprintf(stream,
                            (options.enable_colors)
                            ? _CL_B_BLUE("%c")
                            : "%c",
                            ((char*)buf)[idx]);
                } else {
                    // Otherwise
                    fprintf(stream, ".");
                }
            } else {
                fprintf(stream, " ");
            }
        }
        fprintf(stream, "\n");
    }
}

static void __attribute__((unused))
            print_hex_to_buf(char** output_buf,
                             size_t* output_buf_len,
                             const void* buf,
                             size_t buflen,
                             struct print_hex_options options)
{
    FILE* stream;
    stream = open_memstream(output_buf, output_buf_len);
    print_hex_stream(stream, buf, buflen, options);
    fflush(stream);
    fclose(stream);
}

// LCOV_EXCL_STOP

