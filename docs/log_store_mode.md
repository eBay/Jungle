Log Store Mode
==============

Jungle supports *Log Store Mode*, for those who want functionalities of storing/fetching records by sequence number. Once log store mode is on, only log section (MemTables and log files) is available while underlying tables are disabled. User can both read and write records by key or sequence number. In log store mode, Jungle provides very lightweight *log compaction*, which discards all records from the beginning (i.e., the smallest sequence number) to the given sequence number.

Since there can be a million records in log section, we should maintain hundreds of log files, but their MemTables cannot be in memory together. Jungle evicts MemTables that are not accessed log time, and reconstructs them if user attempts to read any records in that log file again.

Jungle's log store mode is suitable for below use cases:
* Appending records with monotonically increasing sequence number.
* Tail locality: most likely records that are appended recently (i.e., tail) will be read.
* Sequential reads in sequence number order.
* Periodic log compaction: discarding old records in front (i.e., head).

Note that random point query with key or sequence number is still possible, but we don't recommend doing that as it will be very inefficient.

Switching a Jungle instance used for log store mode to normal DB mode is not possible, and vice versa.

Please refer to [example code](../examples/example_log_store_mode.cc).
