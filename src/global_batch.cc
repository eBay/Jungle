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

#include <libjungle/jungle.h>

#include "db_internal.h"

namespace jungle {

bool GlobalBatch::BatchPairLess::operator()
     (const BatchPair& ll, const BatchPair& rr) const
{
    return (ll.db->getPath() < rr.db->getPath());
}

GlobalBatch::GlobalBatch() {}

GlobalBatch::GlobalBatch(const std::list< DB* >& db_list,
                         const std::list< std::list<Record> >& batch_list)
{
    if (!db_list.size() || db_list.size() != batch_list.size()) return;

    auto e1 = db_list.begin();
    auto e2 = batch_list.begin();
    while (e1 != db_list.end()) {
        addBatch(*e1, *e2);
        e1++;
        e2++;
    }
}

GlobalBatch& GlobalBatch::addBatch(DB* db, const std::list<Record>& batch) {
    // Should ignore read-only DB.
    if (db->isReadOnly()) return *this;

    BatchPair new_pair = {db, batch};
    auto entry = pairList.find(new_pair);
    if (entry != pairList.end()) {
        // We do not allow duplicate DB in a batch.
        return *this;
    }
    pairList.insert(new_pair);
    return *this;
}

Status GlobalBatch::execute() {
    DBMgr* dbm = DBMgr::getWithoutInit();
    if (!dbm) return Status::NOT_INITIALIZED;

    return dbm->getGlobalBatchExecutor()->execute(*this);
}

Status GlobalBatchExecutor::execute(const GlobalBatch& g_batch) {
    Status s;

    // Lock log managers.
    for (auto& entry: g_batch.pairList) {
        const GlobalBatch::BatchPair& pair = entry;
        pair.db->p->logMgr->lockWriteMutex();
    }

    // Get max seq number and set barrier for each DB.
    for (auto& entry: g_batch.pairList) {
        const GlobalBatch::BatchPair& pair = entry;
        uint64_t max_seq = 0;
        pair.db->p->logMgr->getMaxSeqNum(max_seq);
        pair.db->p->logMgr->setVisibleSeqBarrier(max_seq);
    }

    // Auto unlock + clearing barrier.
    GcFunc auto_cleanup( [&]() {
        for (auto& entry: g_batch.pairList) {
            const GlobalBatch::BatchPair& pair = entry;
            // WARNING:
            //   Should clean-up visible seq barrier first.
            pair.db->p->logMgr->setVisibleSeqBarrier(0);
            pair.db->p->logMgr->setGlobalBatch(0, nullptr);
            // Unlock should be done at the end.
            pair.db->p->logMgr->unlockWriteMutex();
        }
    } );

    // Validity checking.
    for (auto& entry: g_batch.pairList) {
        const GlobalBatch::BatchPair& pair = entry;
        EP( pair.db->p->logMgr->checkBatchValidity(pair.batch) );
    }

    std::list<uint64_t> elapsed_times;

    std::shared_ptr<GlobalBatchStatus> gb_status = std::make_shared<GlobalBatchStatus>();
    gb_status->batchId = gbIdCounter.fetch_add(1);
    gb_status->curStatus = GlobalBatchStatus::INVISIBLE;

    // Set multi.
    for (auto& entry: g_batch.pairList) {
        Timer tt;
        const GlobalBatch::BatchPair& pair = entry;
        uint64_t new_max_seq = 0;
        s = pair.db->p->logMgr->setMultiInternal(pair.batch, new_max_seq);
        // Record elapsed time of each DB for throttling.
        elapsed_times.push_back(tt.getUs());
        if (s) {
            // Set global batch status.
            pair.db->p->logMgr->setGlobalBatch(new_max_seq, gb_status);
        } else {
            // TODO: Rollback.
        }
    }

    // Set batch of all DBs is done, now make flag VISIBLE, and do clean-up.
    // After this line, all batch items become visible atomically.
    gb_status->curStatus.store(GlobalBatchStatus::VISIBLE, MOR);

    // Unlock and do throttling if necessary.
    auto_cleanup.gcNow();
    auto time_itr = elapsed_times.begin();
    for (auto& entry: g_batch.pairList) {
        const GlobalBatch::BatchPair& pair = entry;
        pair.db->p->logMgr->execBackPressure(*time_itr);
        time_itr++;
    }

    return Status();
}

}

