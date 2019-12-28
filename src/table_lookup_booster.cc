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

#include "table_lookup_booster.h"

#include "table_file.h"
#include "table_mgr.h"

#include _MACRO_TO_STR(LOGGER_H)

namespace jungle {

TableLookupBooster::TableLookupBooster(uint64_t limit,
                                       const TableMgr* table_mgr,
                                       const TableFile* table_file)
    : curLimit(limit)
    , tMgr(table_mgr)
    , tFile(table_file)
    , elems(curLimit)
    , elemsLock(17)
{
    // Will be used in the future.
    (void)tMgr;
    (void)tFile;
}

TableLookupBooster::~TableLookupBooster() {
}

size_t TableLookupBooster::size() const {
    return elems.size();
}

Status TableLookupBooster::get(uint32_t h_value, uint64_t& offset_out) {
    size_t lock_idx = h_value % elemsLock.size();
    size_t elem_idx = h_value % elems.size();

    std::lock_guard<std::mutex> l( elemsLock[lock_idx] );
    Elem& existing = elems[elem_idx];
    if (existing.hValue() == h_value) {
        offset_out = existing.offset();
        return Status();
    }
    return Status::KEY_NOT_FOUND;
}

Status TableLookupBooster::setIfNew(const TableLookupBooster::Elem& elem) {
    size_t lock_idx = elem.hValue() % elemsLock.size();
    size_t elem_idx = elem.hValue() % elems.size();

    std::lock_guard<std::mutex> l( elemsLock[lock_idx] );
    Elem& existing = elems[elem_idx];
    if ( existing.hValue() == elem.hValue() &&
         existing.seqNum() >= elem.seqNum() ) {
        return Status::ALREADY_EXIST;
    }

    elems[elem_idx] = elem;
    return Status();
}

}

