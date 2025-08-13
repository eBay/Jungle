#!/bin/bash
set -e

VALGRIND=

for var in "$@"
do
    if [ $var == "valgrind" ]; then
        VALGRIND="valgrind --undef-value-errors=no"
        echo "valgrind mode: ON"
    fi
    if [ $var == "directio" ]; then
        export TEST_ENABLE_DIRECTIO=true
        echo "direct IO mode: ON"
    fi
done

$VALGRIND ./tests/crc32_test --abort-on-failure
$VALGRIND ./tests/keyvalue_test --abort-on-failure
$VALGRIND ./tests/fileops_test --abort-on-failure
$VALGRIND ./tests/fileops_directio_test --abort-on-failure
$VALGRIND ./tests/memtable_test --abort-on-failure
$VALGRIND ./tests/table_test --abort-on-failure
$VALGRIND ./tests/table_lookup_booster_test --abort-on-failure

$VALGRIND ./tests/basic_op_test --abort-on-failure
$VALGRIND ./tests/sync_and_flush_test --abort-on-failure
$VALGRIND ./tests/nearest_search_test --abort-on-failure
$VALGRIND ./tests/seq_itr_test --abort-on-failure
$VALGRIND ./tests/key_itr_test --abort-on-failure
$VALGRIND ./tests/snapshot_test --abort-on-failure
$VALGRIND ./tests/custom_cmp_test --abort-on-failure
$VALGRIND ./tests/corruption_test --abort-on-failure
$VALGRIND ./tests/compaction_test --abort-on-failure
$VALGRIND ./tests/mt_test --abort-on-failure
$VALGRIND ./tests/log_reclaim_test --abort-on-failure
$VALGRIND ./tests/level_extension_test --abort-on-failure
$VALGRIND ./tests/compression_test --abort-on-failure
$VALGRIND ./tests/atomic_batch_test --abort-on-failure
$VALGRIND ./tests/builder_test --abort-on-failure
$VALGRIND ./tests/disabling_seq_index_test --abort-on-failure
