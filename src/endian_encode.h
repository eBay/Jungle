/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.

Original Copyright 2016 Couchbase, Inc.
See URL: https://github.com/couchbase/forestdb

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

#if defined(WIN32) || defined(_WIN32)
    #ifndef _LITTLE_ENDIAN
    #define _LITTLE_ENDIAN
    #endif

#elif __linux__
    #include <endian.h>
    #if __BYTE_ORDER == __LITTLE_ENDIAN
        #ifndef _LITTLE_ENDIAN
        #define _LITTLE_ENDIAN
        #endif
    #elif __BYTE_ORDER == __BIG_ENDIAN
        #ifndef _BIG_ENDIAN
        #define _BIG_ENDIAN
        #endif
    #else
        #error "not supported platform"
    #endif

#elif __APPLE__
    #include <machine/endian.h>
    #if BYTE_ORDER == LITTLE_ENDIAN
        #ifndef _LITTLE_ENDIAN
        #define _LITTLE_ENDIAN
        #endif
    #elif BYTE_ORDER == BIG_ENDIAN
        #ifndef _BIG_ENDIAN
        #define _BIG_ENDIAN
        #endif
    #else
        #error "not supported endian"
    #endif

#else
    #error "not supported platform"

#endif

#ifndef reverse_order_64
#define reverse_order_64(v)    \
    ( (((v) & 0xff00000000000000ULL) >> 56) \
    | (((v) & 0x00ff000000000000ULL) >> 40) \
    | (((v) & 0x0000ff0000000000ULL) >> 24) \
    | (((v) & 0x000000ff00000000ULL) >>  8) \
    | (((v) & 0x00000000ff000000ULL) <<  8) \
    | (((v) & 0x0000000000ff0000ULL) << 24) \
    | (((v) & 0x000000000000ff00ULL) << 40) \
    | (((v) & 0x00000000000000ffULL) << 56) )
#endif

#ifndef reverse_order_32
#define reverse_order_32(v)    \
    ( (((v) & 0xff000000) >> 24) \
    | (((v) & 0x00ff0000) >>  8) \
    | (((v) & 0x0000ff00) <<  8) \
    | (((v) & 0x000000ff) << 24) )
#endif

#ifndef reverse_order_16
#define reverse_order_16(v)    \
    ( (((v) & 0xff00) >> 8) \
    | (((v) & 0x00ff) << 8) )
#endif

#if defined(_LITTLE_ENDIAN)
    // convert to big endian
    #define _enc64(v) reverse_order_64(v)
    #define _dec64(v) reverse_order_64(v)
    #define _enc32(v) reverse_order_32(v)
    #define _dec32(v) reverse_order_32(v)
    #define _enc16(v) reverse_order_16(v)
    #define _dec16(v) reverse_order_16(v)
#else
    // big endian .. do nothing
    #define _enc64(v) (v)
    #define _dec64(v) (v)
    #define _enc32(v) (v)
    #define _dec32(v) (v)
    #define _enc16(v) (v)
    #define _dec16(v) (v)
#endif

#define __ENDIAN_SAFE
#ifdef __ENDIAN_SAFE
#define _enc(v) \
    ((sizeof(v) == 8)?(_enc64(v)):( \
     (sizeof(v) == 4)?(_enc32(v)):( \
     (sizeof(v) == 2)?(_enc16(v)):(v))))
#define _dec(v) \
    ((sizeof(v) == 8)?(_dec64(v)):( \
     (sizeof(v) == 4)?(_dec32(v)):( \
     (sizeof(v) == 2)?(_dec16(v)):(v))))
#else
#define _enc(v) (v)
#define _dec(v) (v)
#endif

