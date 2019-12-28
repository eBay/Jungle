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

#include "test_common.h"

#include "dist_def.h"
#include "json_common.h"
#include "json_to_dist_def.h"

#include <algorithm>
#include <list>
#include <map>
#include <unordered_map>

using namespace jungle_bench;

namespace dist_def_test {

void prepare_sample_set(DistDef::Type type,
                        int size,
                        int zipf_n,
                        int mean,
                        int sigma,
                        double alpha,
                        std::map<uint64_t, uint64_t>& map)
{
    json::JSON params;

    switch (type) {
    case DistDef::Type::NORMAL:
        params["type"] = "normal";
        break;
    case DistDef::Type::RANDOM:
    default:
        params["type"] = "random";
        break;
    case DistDef::Type::ZIPF:
        params["type"] = "zipf";
        break;
    }

    params["alpha"] = alpha;
    params["n"] = zipf_n;
    params["median"] = mean;
    params["sigma"] = sigma;

    DistDef dist = load_dist_def_from_json(params);
    TestSuite::_msg(dist.toString().c_str());
    for (int i = 0; i < size; ++i) {
        uint64_t val =
            (type != DistDef::Type::ZIPF)
            ? dist.get() : dist.getZipfElem();
        if (map.find(val) == map.end()) {
            map[val] = 0;
        }
        map[val]++;
    }
}

double get_mean(const std::map<uint64_t, uint64_t>& map) {
    uint64_t total_sum = 0;
    uint64_t total_size = 0;
    for (auto iter: map) {
        total_sum += iter.first * iter.second;
        total_size += iter.second;
    }
    return (double) (total_sum / total_size);
}

double get_sigma(const std::map<uint64_t, uint64_t>& map) {
    double mean = get_mean(map);
    double sigma = 0.0;
    uint64_t total_sum = 0;
    for (auto iter: map) {
        uint64_t i = iter.second;
        for (uint64_t j = 0; j < i; ++j) {
            sigma += pow((mean - iter.first), 2);
        }
        total_sum += iter.second;
    }
    return sqrt(sigma / total_sum);
}

uint64_t get_min(const std::map<uint64_t, uint64_t>& map) {
    auto itr = map.begin();
    if (itr == map.end()) return 0;
    return itr->first;
}

uint64_t get_max(const std::map<uint64_t, uint64_t>& map) {
    auto itr = map.rbegin();
    if (itr == map.rend()) return 0;
    return itr->first;
}

double get_cdf(const std::map<uint64_t, uint64_t>& map, uint64_t x) {
    uint64_t cnt = 0;
    uint64_t total_sum = 0;
    for (auto iter: map) {
        if (iter.first <= x) {
            cnt += iter.second;
        }
        total_sum += iter.second;
    }
    return (cnt / (double)total_sum);
}

int random_dist_test(const std::vector<int>& sizes,
                     uint64_t median,
                     uint64_t sigma)
{
    std::map<uint64_t, uint64_t> mymap;
    TestSuite::_msg("Random Distribution: \n");
    for (int size: sizes) {
        mymap.clear();
        TestSuite::_msg("Sample Set Size = %d\n", size);
        prepare_sample_set(DistDef::Type::RANDOM, size, 0, median, sigma, 0, mymap);
        TestSuite::_msg("\nMean: %f", get_mean(mymap));
        TestSuite::_msg("\nSD: %f", get_sigma(mymap));
        TestSuite::_msg("\nMin: %zu", get_min(mymap));
        TestSuite::_msg("\nMax: %zu", get_max(mymap));
        for (uint64_t ii=1; ii<=10; ++ii) {
            // Random: 100% value should be in
            //         [median - sigma, median + sigma).
            uint64_t val = (median - sigma) + (sigma * 2) * ii / 10;
            TestSuite::_msg( "\nCDF <= %zu: %f",
                             val, get_cdf(mymap, val) );
        }
        TestSuite::_msg("\n\n");
    }
    return 0;
}

int normal_dist_test(const std::vector<int>& sizes,
                     uint64_t median,
                     uint64_t sigma)
{
    std::map<uint64_t, uint64_t> mymap;
    TestSuite::_msg("Normal Distribution: \n");
    for (int size: sizes) {
        mymap.clear();
        TestSuite::_msg("Sample Set Size = %d\n", size);
        prepare_sample_set(DistDef::Type::NORMAL, size, 0, median, sigma, 0, mymap);
        TestSuite::_msg("\nMean: %f", get_mean(mymap));
        TestSuite::_msg("\nSD: %f", get_sigma(mymap));
        TestSuite::_msg("\nMin: %zu", get_min(mymap));
        TestSuite::_msg("\nMax: %zu", get_max(mymap));
        for (uint64_t ii=1; ii<=12; ++ii) {
            // Normal: 99.7% value should be in
            //         [median - 3*sigma, median + 3*sigma].
            uint64_t val = (median - 3 * sigma) + (sigma * 6) * ii / 12;
            TestSuite::_msg( "\nCDF <= %zu: %f",
                             val, get_cdf(mymap, val) );
        }
        TestSuite::_msg("\n\n");
    }
    return 0;
}

int zipf_dist_test(const std::vector<int>& sizes,
                   uint64_t zipf_n,
                   const std::vector<double>& alphas)
{
    std::map<uint64_t, uint64_t> mymap;
    TestSuite::_msg("Zipfian Distribution: \n");
    for (int size: sizes) {
        TestSuite::_msg("Sample Set Size = %d\n", size);
        for (double alpha: alphas) {
            mymap.clear();
            prepare_sample_set(DistDef::Type::ZIPF, size, zipf_n, 0, 0, alpha, mymap);
            TestSuite::_msg("\nMean: %f", get_mean(mymap));
            TestSuite::_msg("\nSD: %f", get_sigma(mymap));
            TestSuite::_msg("\nMin: %zu", get_min(mymap));
            TestSuite::_msg("\nMax: %zu", get_max(mymap));
            for (uint64_t ii=1; ii<=10; ++ii) {
                uint64_t val = zipf_n * ii / 10;
                TestSuite::_msg( "\nCDF <= %zu: %f",
                                 val, get_cdf(mymap, val) );
            }
            TestSuite::_msg("\n\n");
        }
        TestSuite::_msg("\n\n");
    }
    return 0;
}

int deterministic_random_test(size_t NUM) {
    std::list<uint64_t> random_numbers;
    DistDef dd(DistDef::RANDOM, NUM, NUM);

    // 1st run: collect random numbers.
    for (size_t ii=0; ii<NUM; ++ii) {
        random_numbers.push_back( dd.get(ii) );
    }

    // 2nd run: check if it generates identical random numbers.
    auto entry = random_numbers.begin();
    for (size_t ii=0; ii<NUM; ++ii) {
        uint64_t expected_number = *entry;
        uint64_t regen_number = dd.get(ii);
        CHK_EQ(expected_number, regen_number);
        entry++;
    }
    return 0;
}

int deterministic_normal_dist_test(size_t NUM) {
    std::list<uint64_t> random_numbers;
    DistDef dd(DistDef::NORMAL, NUM, NUM/10);

    // 1st run: collect random numbers.
    for (size_t ii=0; ii<NUM; ++ii) {
        random_numbers.push_back( dd.get(ii) );
    }

    // 2nd run: check if it generates identical random numbers.
    auto entry = random_numbers.begin();
    for (size_t ii=0; ii<NUM; ++ii) {
        uint64_t expected_number = *entry;
        uint64_t regen_number = dd.get(ii);
        CHK_EQ(expected_number, regen_number);
        entry++;
    }
    return 0;
}

int deterministic_zipf_dist_test(size_t NUM) {
    std::list<uint64_t> random_numbers;
    DistDef dd(DistDef::ZIPF, NUM, NUM, 1.0, 10);

    // 1st run: collect random numbers.
    for (size_t ii=0; ii<NUM; ++ii) {
        random_numbers.push_back( dd.getZipfElem(ii) );
    }

    // 2nd run: check if it generates identical random numbers.
    std::map<uint64_t, uint64_t> count;
    auto entry = random_numbers.begin();
    for (size_t ii=0; ii<NUM; ++ii) {
        uint64_t expected_number = *entry;
        uint64_t regen_number = dd.getZipfElem(ii);
        CHK_EQ(expected_number, regen_number);
        entry++;
        count[expected_number]++;
    }

    size_t cumul = 0;
    for (auto& entry: count) {
        cumul += entry.second;
        TestSuite::_msg("%zu\t%zu\t%zu\n", entry.first, entry.second, cumul);
    }

    return 0;
}

int deterministic_zipf_dist_test2(size_t NUM) {
    std::list<uint64_t> random_numbers;
    size_t RANGE = 1000;
    DistDef dd(DistDef::ZIPF, RANGE / 2, RANGE / 2, 1.0, 10);

    // 1st run: collect random numbers.
    for (size_t ii=0; ii<NUM; ++ii) {
        random_numbers.push_back( dd.get(ii) );
    }

    // 2nd run: check if it generates identical random numbers.
    std::map<uint64_t, uint64_t> freq_count;
    auto entry = random_numbers.begin();
    for (size_t ii=0; ii<NUM; ++ii) {
        uint64_t expected_number = *entry;
        uint64_t regen_number = dd.get(ii);
        CHK_EQ(expected_number, regen_number);
        entry++;
        freq_count[expected_number]++;
    }

    struct Elem {
        Elem(uint64_t c = 0, uint64_t n = 0) : count(c), number(n) {}
        uint64_t count;
        uint64_t number;
    };

    std::vector<Elem> elems;
    for (auto& entry: freq_count) {
        elems.push_back( Elem(entry.second, entry.first) );
    }
    std::sort( elems.begin(), elems.end(),
               [](const Elem& a, const Elem& b) -> bool {
                   return (a.count > b.count);
               } );

    size_t sum = 0;
    size_t id = 0;
    double last_print = 0.0;
    bool print_all = false;
    for (const Elem& entry: elems) {
        sum += entry.count;
        id++;
        if ( (id * 100.0 / elems.size()) >= last_print + 10 ||
             print_all ) {
            TestSuite::_msg("%zu\t%zu\t(%.1f)\t%zu\t%zu\n",
                            sum, id, (id * 100.0 / elems.size()),
                            entry.count, entry.number);
            last_print += 10;
        }
    }

    return 0;
}

}

using namespace dist_def_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = true;

    std::vector<int> sizes { 1000, 10000, 100000 };
    int zipf_n = 10000;
    std::vector<double> alphas { 0.1, 0.27, 0.99 };
    uint64_t median = 5000, sigma = 5000;

    ts.doTest( "uniform random test",
               random_dist_test,
               sizes, median, sigma );

    ts.doTest( "normal distribution test",
               normal_dist_test,
               sizes, median, sigma/4 );

    ts.doTest( "zipfian distribution test",
               zipf_dist_test,
               sizes, zipf_n, alphas );

    ts.doTest( "deterministic uniform random test",
               deterministic_random_test,
               100000 );

    ts.doTest( "deterministic normal distribution test",
               deterministic_normal_dist_test,
               100000 );

    ts.doTest( "deterministic zipf distribution test",
               deterministic_zipf_dist_test,
               100000 );

    ts.doTest( "deterministic zipf distribution test2",
               deterministic_zipf_dist_test2,
               100000 );

    return 0;
}
