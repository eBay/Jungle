Overview: Combining LSM-Tree & Append-Only B+Tree
=================================================

Similar to other popular LSM-tree implementations, Jungle adopts horizontally partitioned leveling approach; there are multiple levels whose maximum size increases exponentially, and each level consists of multiple disjoint (i.e., non-overlapping) tables. Below diagram illustrates the overall architecture.

```
          [MemTable 0]  [MemTable 1]  ...                     memory
--------------------------------------------------------------------
Log       [Log file 0]  [Log file 1]  ...                     disk

Level-0   [Table-hash(0)]  [Table-hash(1)]  ...
(hash)

Level-1   [Table-range(0-100)]  [Table-range(101-200)]  ...
(range)

Level-2   [Table-range(0-10)]  [Table-range(11-20)]  ...
(range)

...
```

Once a new update comes in, it is inserted into *MemTable*, which is an in-memory ordered index based on lock-free [Skiplist](https://en.wikipedia.org/wiki/Skip_list). Each MemTable has a corresponding log file on disk, and the contents of MemTables are periodically synced to log file for durability. Records in a log file are stored in chronological order, so that we always need corresponding MemTables to serve `get` operation in `O(log N)` time. Different log files or MemTables may have duplicate key.

Each MemTable has a limit on both size and the number of records. A new MemTable and log file pair is created if the last one becomes full. Only the latest MemTable serves the incoming traffic, while older ones become immutable.

Since MemTables reside in memory, there is a limit for the entire size of MemTable and log file pairs, and the records in MemTables are merged into Tables in level-0 once the size exceeds the limit, or user explicitly requests the task. After merge, stale log files and MemTables are removed.

A *Table* is an ordered index on disk, where each table is a single file. Tables in the same level are not overlapping each other, thus an arbitrary key belongs to only one table in a level. Tables in different levels can have duplicate key, but the key in upper (i.e., the smaller number) level is always newer than that in lower level.

Each level has a size limit. Once a level exceeds the limit, Jungle picks a victim Table, and merges it to overlapping Tables in the next level, called *inter-level compaction*. If no more level exists, there are two choices: 1) if the entire DB size is big enough, add one more level. Otherwise 2) do *in-place compaction* to reduce the size. Please refer to our [paper](https://www.usenix.org/conference/hotstorage19/presentation/ahn) for more details.


Jungle Table vs. Sorted String Table (SSTable)
----------------------------------------------
In Jungle, each individual Table in every level is an append-only B+tree, whose key and value are separated. We are currently using ForestDB for it. Data once written in append-only B+tree are immutable, similar to sorted string tables (SSTables) in other LSM-tree implementations, thus it does not allow overwriting existing data. That means we can support multi-version concurrency control (MVCC) as other implementations do; writers do not block readers, and readers do not writers.

However, one big advantage of append-only B+tree over SSTable is that it still supports *append* operation while keeping previous data immutable. If we want to merge some records into a SSTable, we should write a brand new SSTable including the new records, and then remove the old SSTable. With append-only B+tree, all we need to do for merge is just appending new records (i.e., delta) to the existing table, while append-only B+tree will take care of the ordering of the table so that we are still able to do `O(log N)` search.

A notable benefit of doing *append* instead of *rewrite* is reducing write amplification. The amount of disk writes for rewriting SSTables is huge compared to the original amount of the data to update. Append-only B+tree also requires periodic *compaction* task which rewrites the entire B+tree to reclaim disk space. However, the frequency of append-only B+tree compaction is much less than that of SSTable rewriting. In our observation, the overall write amplification is reduced by up to 4-5 times compared to other LSM-tree implementations.

Other benefits:
* We have more chances for aggressive optimizations. For instance, the level-0 in Jungle is hash-partitioned to boost parallel disk write (i.e., appending data to Tables in level-0), without increasing read cost by keeping all tables disjoint.<br>
In other LSM-tree implementations, level-0 is a special level where key overlapping is allowed, to make MemTable flushing faster by avoiding rewriting existing SSTables. But due to duplicate keys across different SSTables, 1) searching a key in level-0 and 2) merging data from level-0 to level-1 might be inefficient.

* We can still use the original benefits of append-only B+tree, where every individual append works as a persistent snapshot. Having snapshots that are persistent across process restarts is difficult to achieve in existing LSM-tree implementations, due to SSTable characteristics. However, Jungle can easily support persistent snapshots as each Table is an append-only B+tree.


Reducing the Number of Levels
-----------------------------
Typically, the ratio of size limit between adjacent levels is a fixed number in most LSM-tree implementations, usually `10`. This number directly affects 1) the number of levels and 2) the overall write amplification. If we have bigger ratio number, it will reduce the number of levels which is good for decreasing read cost. However, it increases the number of overlapping SSTables between adjacent levels, that means write amplification also increases proportionally as we should rewrite all overlapping SSTables.

However, in Jungle, we only append delta to Tables in the next level. Consequently, increasing the ratio number barely affects the write amplification so that we can safely reduce the number of levels, to get better read performance.

