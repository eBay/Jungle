/************************************************************************
Copyright 2017-2020 eBay Inc.

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

#include "record.h"
#include "status.h"

#include <list>
#include <set>

namespace jungle {

class DB;
class GlobalBatchExecutor;
class GlobalBatch {
    friend class GlobalBatchExecutor;
public:
    /**
     * Pair of DB and its batch.
     */
    struct BatchPair {
        /**
         * DB instance.
         */
        DB* db;

        /**
         * Batch.
         */
        std::list<Record> batch;
    };

    GlobalBatch();

    /**
     * Initialize a global batch with given parameters.
     *
     * @param db List of DB instances.
     * @param batch List of records sets for each DB.
     */
    GlobalBatch(const std::list<DB*>& db_list,
                const std::list< std::list<Record> >& batch_list);

    /**
     * Add DB and its corresponding batch pair.
     *
     * @param db DB instance.
     * @param batch List of records to set.
     * @return Global batch instance.
     */
    GlobalBatch& addBatch(DB* db, const std::list<Record>& batch);

    /**
     * Execute the batch atomically.
     */
    Status execute();

protected:
    /**
     * Less comparator.
     */
    struct BatchPairLess {
        bool operator() (const BatchPair& ll, const BatchPair& rr) const;
    };

    /**
     * List of DB handles and their corresponding batches,
     * sorted by the path of DB.
     */
    std::set<BatchPair, BatchPairLess> pairList;
};

}

