Understanding Sequence Numbers
==============================

In Jungle, every single mutation (i.e., set, delete, and user-defined special record) has a unique sequence number, which starts from `1` and then monotonically increasing. For example, if we set three keys, their sequence numbers will be `1`, `2`, and `3`, respectively:
```C++
db->set( KV("a", "A") );    // sequence number 1.
db->set( KV("b", "B") );    // sequence number 2.
db->set( KV("c", "C") );    // sequence number 3.
```

If we delete an existing key `b`, the new sequence number `4` will be assigned to the deletion marker, called *tombstone*:
```C++
db->del( SizedBuf("b") );   // sequence number 4.
```
This tombstone will remain until the underlying table gets compacted. After compaction, both key `"b"` and sequence number `4` will not be visible.

If multiple mutations are issued on the same key, multiple sequence numbers will be created for it. But eventually only the last sequence number of the key will last as logs and Tables gets merged:
```C++
db->set( KV("b", "BB") );   // sequence number 5.
db->del( SizedBuf("b") );   // sequence number 6.
db->set( KV("b", "BBB") );  // sequence number 7.
```
Sequence numbers for the key `"b"` (`2` and `4`-`7`) will exist for a while, and finally only the last sequence number `7` will remain. Note that the record referred to by the last sequence number always represents the current state of the key, which means that applying the sequence of mutations from `4` to `7` and applying only the mutation `7` will bring you the same result.

