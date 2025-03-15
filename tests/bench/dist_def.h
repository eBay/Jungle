/************************************************************************
Copyright 2017-2019 eBay Inc.

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

#include "murmurhash3.h"

#include <cmath>
#include <limits>
#include <map>
#include <random>
#include <sstream>
#include <string>

#include <assert.h>
#include <string.h>

struct DistDef {
    enum Type {
        RANDOM = 0x0,
        NORMAL = 0x1,
        ZIPF   = 0x2,
    };

    DistDef( Type _type = RANDOM,
             uint64_t _median = 50,
             uint64_t _sigma = 50,
             double _alpha = 1.0,
             uint64_t _z_elems = 10 )
        : type(_type)
        , median(_median)
        , sigma(_sigma)
        , zAlpha(_alpha)
        , zNumElems(_z_elems)
        , zPrimeNumber(7)
    {
        if (type == ZIPF) {
            // Prepare the probabilities.
            double c = 0;
            for (uint64_t ii = 1; ii <= zNumElems; ++ii) {
                c = c + ( 1.0 / pow( (double)ii, zAlpha) );
            }
            c = 1.0 / c;

            // Reverse calculate the cumulative probability, and keep it.
            double x = 0.0;
            for (uint64_t ii = 1; ii <= zNumElems; ++ii) {
                x += c / pow((double)ii, zAlpha);

                // The key of last `probs` entry MUST BE 1.0.
                if (ii == zNumElems) x = 1.0;
                zProbs[x] = ii;
            }

            // Find proper prime number according to `sigma * 2`.
            static std::vector<uint64_t>
                prime_numbers = {53, 503, 5003, 50021, 500009, 5000011};
            for (uint64_t p: prime_numbers) {
                if (p < sigma * 2) zPrimeNumber = p;
            }
        }
    }

    ~DistDef() {}

    /**
     * Return a random number according to configured distribution.
     *
     * @return Random number.
     */
    uint64_t get() const {
        return get( std::rand() );
    }

    /**
     * Return the index of Zipfian element according to
     * configured parameter (i.e., alpha).
     *
     * @return Zipfian element index number: [0, zNumElems).
     */
    uint64_t getZipfElem() const {
        if (type != ZIPF) return 0;
        return getZipfElem( std::rand() );
    }

    /**
     * Calculate random number from given index deterministically.
     * If the same index is given, it returns the same random number.
     *
     * @param index Seed number to generate random number.
     * @return Random number.
     */
    uint64_t get(uint64_t index) const {
        static uint64_t MAX64 = std::numeric_limits<uint64_t>::max();
        static double PI = 3.141592654;

        // Generate two u64 random numbers.
        uint64_t rr[2];
        MurmurHash3_x64_128(&index, sizeof(index), 0, rr);

        switch (type) {
        case NORMAL: {
            double r1 = 0, r2 = 0;
            r1 = -log( 1 - ( (double)rr[0] / MAX64 ) );
            r2 =  2 * PI * ( (double)rr[1] / MAX64 );
            r1 =  sqrt( 2 * r1 );

            int64_t value = (int64_t)sigma * r1 * cos(r2) + median;
            if (value < 0) value = 0;
            return (uint64_t)value;
        }

        case ZIPF: {
            // 1) First get the Zipf group index.
            double z = (double)rr[0] / MAX64;
            auto iter = zProbs.lower_bound(z);
            assert(iter != zProbs.end());
            uint64_t group_idx = iter->second - 1;
            assert(group_idx < zNumElems);

            // 2) Randomly get a random number within range corresponding to
            //    the Zipf group.
            uint64_t range_per_group = sigma * 2 / zNumElems;
            uint64_t next_index = (range_per_group)
                                  ? rr[1] % range_per_group : 0;
            next_index += range_per_group * group_idx;

            // 3) Using `next_index`, do linear shift and find
            //    its corresponding (pseudo random) number,
            //    by linear congruential generation.
            uint64_t local_random = (next_index * zPrimeNumber + 7) %
                                    (sigma * 2);
            return median + local_random - sigma;
        }

        default:
            // Uniform random.
            uint64_t local_random = 0;
            if (sigma) local_random = rr[0] % (sigma * 2);
            return median + local_random - sigma;
        }
        return 0;
    }

    /**
     * Calculate the index of Zipfian element corresponding to the given
     * index. If the same index is given, it returns the same element index.
     *
     * @param index Seed number to get Zipfian element index.
     * @return Zipfian element index number: [0, zNumElems).
     */
    uint64_t getZipfElem(uint64_t index) const {
        if (type != ZIPF) return 0;

        // Generate two u64 random numbers.
        uint64_t rr[2];
        MurmurHash3_x64_128(&index, sizeof(index), 0, rr);

        double z = (double)rr[0] / (double)std::numeric_limits<uint64_t>::max();
        auto iter = zProbs.lower_bound(z);
        assert(iter != zProbs.end());
        return iter->second - 1;
    }

    /**
     * Dump current distribution settings to string.
     *
     * @return Information string.
     */
    std::string toString() const {
        std::stringstream ss;

        if (type == RANDOM || type == NORMAL) {
            if (!sigma) {
                // Fixed length
                ss << median << " (fixed)";
                return ss.str();
            }
        }

        switch (type) {
        case RANDOM:
            ss << "R[" << median - sigma << ", " << median + sigma << ")";
            break;

        case NORMAL:
            ss << "N(" << median << ", " << sigma << ")";
            break;

        case ZIPF:
            ss << "Z(" << zAlpha << ", " << zNumElems << ")";
            break;

        default:
            ss << "unknown";
            break;
        };
        return ss.str();
    }

    // * Uniform random: [median - sigma, median + sigma)
    //
    // * Normal:         Norm(median, sigma)
    //   - 68%: [median -     sigma, median +     sigma]
    //   - 97%: [median - 2 * sigma, median + 2 * sigma]
    //   - 99%: [median - 3 * sigma, median + 3 * sigma]
    //
    // * Zipf:           According to `zAlpha` value.
    //   If `zNumElems` = 10
    //            0    1    2     3 (Zipfian element index)
    //   - 0.0: 10%, 10%, 10%,  10%, ... (even)
    //   - 1.0: 34%, 17%, 11%, 8.5%, ...
    //   - 2.0: 65%, 16%,  7%,   4%, ...
    //   Within the same element, the probability will be uniform random.

    Type type;

    // Expected median.
    uint64_t median;

    // Expected sigma.
    uint64_t sigma;

    // --- For Zipfian distribution ---
    // Zipfian alpha parameter.
    double zAlpha;

    // Number of probability elements for Zipfian distribution.
    uint64_t zNumElems;

    // Cumulative probability map.
    std::map<double, uint64_t> zProbs;

    // Prime number for linear shift.
    uint64_t zPrimeNumber;
};

