#include "sampler.h"

#include "internal_helper.h"

#include <unordered_set>

namespace jungle {

Status Sampler::getSampleKeys(DB* db,
                              const SamplingParams& params,
                              std::list<SizedBuf>& keys_out)
{
    if (!params.numSamples) {
        return Status::INVALID_PARAMETERS;
    }

    Status s;
    Iterator itr;
    itr.initSN(db);

    uint64_t min_seq = 1, max_seq = 0;
    db->getMaxSeqNum(max_seq);
    if (min_seq > max_seq) {
        // NOTE: This happens when the DB is empty, we should return OK with empty list.
        return Status::OK;
    }

    uint64_t num_remaining = params.numSamples;
    uint64_t seq_step = (max_seq - min_seq) / num_remaining;
    uint64_t exp_cur_seq = min_seq + seq_step;
    uint64_t exp_next_seq = exp_cur_seq + seq_step;

    struct SizedBufHash {
        size_t operator()(const SizedBuf& buf) const {
            return get_murmur_hash_32(buf);
        }
    };
    std::unordered_set<SizedBuf, SizedBufHash> found_keys;
    bool stop_sampling = false;

    do {
        s = itr.seekSN(exp_cur_seq);
        if (!s.ok()) break;

        Record rec_found;
        Record::Holder h_rec_found(rec_found);
        do {
            Record rec_out;
            Record::Holder hr(rec_out);
            s = itr.get(rec_out);
            if (!s.ok()) {
                // Fatal error.
                stop_sampling = true;
                break;
            }

            auto entry = found_keys.find(rec_out.kv.key);
            if (entry == found_keys.end()) {
                if (params.liveKeysOnly) {
                    SizedBuf value_out;
                    SizedBuf::Holder hv(value_out);
                    s = db->get(rec_out.kv.key, value_out);
                    if (s.ok()) {
                        // Live key.
                        rec_out.moveTo(rec_found);
                        break;
                    }
                    // Otherwise, it is a deleted key.
                } else {
                    // We don't care whether the key is deleted or not.
                    rec_out.moveTo(rec_found);
                    break;
                }
            }

            // Current key is not acceptable, move one step forward.
            s = itr.next();
            if (!s.ok()) {
                // No more records.
                stop_sampling = true;
                break;
            }
        } while (s.ok());
        if (stop_sampling) break;

        SizedBuf moved_key;
        rec_found.kv.key.moveTo(moved_key);
        keys_out.push_back(moved_key);

        // NOTE: The life cycle of `dup_keys` is shorter than `keys_out`,
        //       thus it should be safe.
        found_keys.insert(moved_key);

        if ( --num_remaining == 0 ||
             rec_found.seqNum >= max_seq) {
            break;
        }

        if (rec_found.seqNum < exp_next_seq) {
            // Sample seq number is within normal range, move forward.
        } else {
            // No sample within the range, adjust the step.
            seq_step = (max_seq - rec_found.seqNum) / num_remaining;
            exp_next_seq = rec_found.seqNum + seq_step;
        }
        exp_cur_seq = exp_next_seq;
        exp_next_seq = exp_cur_seq + seq_step;

    } while (s.ok());

    itr.close();
    return Status::OK;
}

}

