#pragma once

#include <libjungle/jungle.h>

namespace jungle {

class DB;
class Sampler {
public:
    Sampler();
    ~Sampler();

    static Status getSampleKeys(DB* db,
                                const SamplingParams& params,
                                std::list<SizedBuf>& keys_out);
};

}

