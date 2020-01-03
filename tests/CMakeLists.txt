set(TEST_DIR ${PROJECT_SOURCE_DIR}/tests)
set(UNIT_TEST_DIR ${TEST_DIR}/unit)
set(JUNGLE_TEST_DIR ${TEST_DIR}/jungle)
set(STRESS_TEST_DIR ${TEST_DIR}/stress)

set(JUNGLE_TEST_DEPS
    ${CMAKE_CURRENT_BINARY_DIR}/../libjungle.a
    ${LIBSIMPLELOGGER}
    ${FDB_LIB_DIR}/libforestdb.a
    ${LIBSNAPPY}
    ${LIBDL})

set(FILEOPS_TEST ${TEST_DIR}/unit/fileops_test.cc)
add_executable(fileops_test ${FILEOPS_TEST})
target_link_libraries(fileops_test ${JUNGLE_TEST_DEPS})
add_dependencies(fileops_test static_lib)

set(FILEOPS_DIRECTIO_TEST ${TEST_DIR}/unit/fileops_directio_test.cc)
add_executable(fileops_directio_test ${FILEOPS_DIRECTIO_TEST})
target_link_libraries(fileops_directio_test ${JUNGLE_TEST_DEPS})
add_dependencies(fileops_directio_test static_lib)

set(KEYVALUE_TEST ${TEST_DIR}/unit/keyvalue_test.cc)
add_executable(keyvalue_test ${KEYVALUE_TEST})
target_link_libraries(keyvalue_test ${JUNGLE_TEST_DEPS})
add_dependencies(keyvalue_test static_lib)

set(MEMTABLE_TEST ${TEST_DIR}/unit/memtable_test.cc)
add_executable(memtable_test ${MEMTABLE_TEST})
target_link_libraries(memtable_test ${JUNGLE_TEST_DEPS})
add_dependencies(memtable_test static_lib)

set(CRC32_TEST ${TEST_DIR}/unit/crc32_test.cc)
add_executable(crc32_test ${CRC32_TEST})
target_link_libraries(crc32_test ${JUNGLE_TEST_DEPS})
add_dependencies(crc32_test static_lib)

set(TABLE_LOOKUP_BOOSTER_TEST ${TEST_DIR}/unit/table_lookup_booster_test.cc)
add_executable(table_lookup_booster_test ${TABLE_LOOKUP_BOOSTER_TEST})
target_link_libraries(table_lookup_booster_test ${JUNGLE_TEST_DEPS})
add_dependencies(table_lookup_booster_test static_lib)

add_custom_target(unit_test)
add_dependencies(unit_test
                 fileops_test
                 fileops_directio_test
                 keyvalue_test
                 crc32_test)

set(BASIC_OP_TEST ${TEST_DIR}/jungle/basic_op_test.cc)
add_executable(basic_op_test ${BASIC_OP_TEST})
target_link_libraries(basic_op_test ${JUNGLE_TEST_DEPS})
add_dependencies(basic_op_test static_lib)

set(SEQ_ITR_TEST ${TEST_DIR}/jungle/seq_itr_test.cc)
add_executable(seq_itr_test ${SEQ_ITR_TEST})
target_link_libraries(seq_itr_test ${JUNGLE_TEST_DEPS})
add_dependencies(seq_itr_test static_lib)

set(KEY_ITR_TEST ${TEST_DIR}/jungle/key_itr_test.cc)
add_executable(key_itr_test ${KEY_ITR_TEST})
target_link_libraries(key_itr_test ${JUNGLE_TEST_DEPS})
add_dependencies(key_itr_test static_lib)

set(SNAPSHOT_TEST ${TEST_DIR}/jungle/snapshot_test.cc)
add_executable(snapshot_test ${SNAPSHOT_TEST})
target_link_libraries(snapshot_test ${JUNGLE_TEST_DEPS})
add_dependencies(snapshot_test static_lib)

set(CUSTOM_CMP_TEST ${TEST_DIR}/jungle/custom_cmp_test.cc)
add_executable(custom_cmp_test ${CUSTOM_CMP_TEST})
target_link_libraries(custom_cmp_test ${JUNGLE_TEST_DEPS})
add_dependencies(custom_cmp_test static_lib)

set(CORRUPTION_TEST ${TEST_DIR}/jungle/corruption_test.cc)
add_executable(corruption_test ${CORRUPTION_TEST})
target_link_libraries(corruption_test ${JUNGLE_TEST_DEPS})
add_dependencies(corruption_test static_lib)

set(COMPACTION_TEST ${TEST_DIR}/jungle/compaction_test.cc)
add_executable(compaction_test ${COMPACTION_TEST})
target_link_libraries(compaction_test ${JUNGLE_TEST_DEPS})
add_dependencies(compaction_test static_lib)

set(LEVEL_EXT_TEST ${TEST_DIR}/jungle/level_extension_test.cc)
add_executable(level_extension_test ${LEVEL_EXT_TEST})
target_link_libraries(level_extension_test ${JUNGLE_TEST_DEPS})
add_dependencies(level_extension_test static_lib)

set(LOG_RECLAIM_TEST ${TEST_DIR}/jungle/log_reclaim_test.cc)
add_executable(log_reclaim_test ${LOG_RECLAIM_TEST})
target_link_libraries(log_reclaim_test ${JUNGLE_TEST_DEPS})
add_dependencies(log_reclaim_test static_lib)

set(MT_TEST ${TEST_DIR}/jungle/mt_test.cc)
add_executable(mt_test ${MT_TEST})
target_link_libraries(mt_test ${JUNGLE_TEST_DEPS})
add_dependencies(mt_test static_lib)

set(LARGE_TEST ${TEST_DIR}/jungle/large_test.cc)
add_executable(large_test ${LARGE_TEST})
target_link_libraries(large_test ${JUNGLE_TEST_DEPS})
add_dependencies(large_test static_lib)

add_custom_target(func_test)
add_dependencies(func_test
                 basic_op_test
                 seq_itr_test
                 key_itr_test
                 snapshot_test
                 custom_cmp_test
                 mt_test
                 large_test)


set(FLUSH_ST_TEST ${TEST_DIR}/stress/flush_stress_test.cc)
add_executable(flush_stress_test ${FLUSH_ST_TEST})
target_link_libraries(flush_stress_test ${JUNGLE_TEST_DEPS})
add_dependencies(flush_stress_test static_lib)

set(PURGE_ST_TEST ${TEST_DIR}/stress/purge_stress_test.cc)
add_executable(purge_stress_test ${PURGE_ST_TEST})
target_link_libraries(purge_stress_test ${JUNGLE_TEST_DEPS})
add_dependencies(purge_stress_test static_lib)

set(ITR_ST_TEST ${TEST_DIR}/stress/iterator_stress_test.cc)
add_executable(iterator_stress_test ${ITR_ST_TEST})
target_link_libraries(iterator_stress_test ${JUNGLE_TEST_DEPS})
add_dependencies(iterator_stress_test static_lib)

set(COMPACT_ST_TEST ${TEST_DIR}/stress/compactor_stress_test.cc)
add_executable(compactor_stress_test ${COMPACT_ST_TEST})
target_link_libraries(compactor_stress_test ${JUNGLE_TEST_DEPS})
add_dependencies(compactor_stress_test static_lib)

set(LOG_RC_ST_TEST ${TEST_DIR}/stress/log_reclaim_stress_test.cc)
add_executable(log_reclaim_stress_test ${LOG_RC_ST_TEST})
target_link_libraries(log_reclaim_stress_test ${JUNGLE_TEST_DEPS})
add_dependencies(log_reclaim_stress_test static_lib)

set(MANY_LOG_TEST ${TEST_DIR}/stress/many_log_files_test.cc)
add_executable(many_log_files_test ${MANY_LOG_TEST})
target_link_libraries(many_log_files_test ${JUNGLE_TEST_DEPS})
add_dependencies(many_log_files_test static_lib)

add_custom_target(stress_test)
add_dependencies(stress_test
                 flush_stress_test
                 purge_stress_test
                 many_log_files_test)


set(BASIC_ROBUST_CHILD ${TEST_DIR}/robust/basic_robust_child.cc)
add_executable(basic_robust_child ${BASIC_ROBUST_CHILD})
target_link_libraries(basic_robust_child ${JUNGLE_TEST_DEPS})
add_dependencies(basic_robust_child static_lib)

set(DIST_TEST
    ${TEST_DIR}/bench/dist_def_test.cc)
add_executable(dist_test ${DIST_TEST})
target_link_libraries(dist_test ${JUNGLE_TEST_DEPS})
add_dependencies(dist_test static_lib)


# --- Benchmark ---

set(BENCH
    ${TEST_DIR}/bench/bench.cc
    ${TEST_DIR}/bench/db_adapter_jungle.cc)
add_executable(jungle_bench ${BENCH})
set_target_properties(jungle_bench
                      PROPERTIES COMPILE_FLAGS
                      "-DJUNGLE_ADAPTER=1")
target_link_libraries(jungle_bench ${JUNGLE_TEST_DEPS})
add_dependencies(jungle_bench static_lib)

