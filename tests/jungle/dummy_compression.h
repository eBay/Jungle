#include "internal_helper.h"

#include <libjungle/jungle.h>

#ifdef SNAPPY_AVAILABLE
#include "snappy-c.h"
#endif

#ifdef SNAPPY_AVAILABLE // ===== USING SNAPPY =====

ssize_t dummy_get_max_size(jungle::DB* db,
                           const jungle::Record& rec)
{
    return snappy_max_compressed_length(rec.kv.value.size);
}

ssize_t dummy_compress(jungle::DB* db,
                       const jungle::Record& src,
                       jungle::SizedBuf& dst)
{
    size_t len = dst.size;
    int ret = snappy_compress((char*)src.kv.value.data,
                              src.kv.value.size,
                              (char*)dst.data,
                              &len);
    if (ret < 0) return ret;
    return len;
}

ssize_t dummy_decompress(jungle::DB* db,
                         const jungle::SizedBuf& src,
                         jungle::SizedBuf& dst)
{
    size_t uncomp_len = dst.size;
    int ret = snappy_uncompress((char*)src.data,
                                src.size,
                                (char*)dst.data,
                                &uncomp_len);
    if (ret < 0) return ret;
    return uncomp_len;
}

#else  // ===== USING DUMMY NAIVE COMPRESSION =====
// This compression is based on the counting the number of
// consecutive characters, and then we store
// the character (1-byte) + length (1-byte).

ssize_t dummy_get_max_size(jungle::DB* db,
                           const jungle::Record& rec)
{
    return rec.kv.value.size * 2;
}

ssize_t dummy_compress(jungle::DB* db,
                       const jungle::Record& src,
                       jungle::SizedBuf& dst)
{
    if (!src.kv.value.size) return 0;

    size_t n_consecutive = 0;
    uint8_t cur_char = 0x0;

    jungle::RwSerializer rws(src.kv.value);
    jungle::RwSerializer rws_out(dst);

    while (rws.available(1)) {
        if (!n_consecutive) {
            cur_char = rws.getU8();
            n_consecutive = 1;
            continue;
        }

        uint8_t new_char = rws.getU8();
        bool flush = false;
        if (cur_char == new_char) {
            n_consecutive++;
            if (n_consecutive > 255) {
                n_consecutive--;
                flush = true;
            }
        } else {
            flush = true;
        }

        if (flush) {
            rws_out.putU8(cur_char);
            rws_out.putU8(n_consecutive);

            cur_char = new_char;
            n_consecutive = 1;
        }
    }

    if (n_consecutive) {
        rws_out.putU8(cur_char);
        rws_out.putU8(n_consecutive);
    }
    return rws_out.pos();
}

ssize_t dummy_decompress(jungle::DB* db,
                         const jungle::SizedBuf& src,
                         jungle::SizedBuf& dst)
{
    if (!src.size) return 0;

    jungle::RwSerializer rws(src);
    jungle::RwSerializer rws_out(dst);

    while (rws.available(1)) {
        uint8_t cur_char = rws.getU8();
        size_t n_consecutive = rws.getU8();

        for (size_t ii = 0; ii < n_consecutive; ++ii) {
            rws_out.putU8(cur_char);
        }
    }
    return rws_out.pos();
}
#endif