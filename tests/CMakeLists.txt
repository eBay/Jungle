set(TEST_DIR ${PROJECT_SOURCE_DIR}/tests)
set(UNIT_TEST_DIR ${TEST_DIR}/unit)
set(JUNGLE_TEST_DIR ${TEST_DIR}/jungle)
set(STRESS_TEST_DIR ${TEST_DIR}/stress)

set(JUNGLE_TEST_DEPS
    static_lib
    ${LIBSIMPLELOGGER}
    ${FORESTDB}
    ${LIBSNAPPY})


function(unit_test)
    set(options SKIP)
    set(oneValueArgs NAME TIMEOUT)
    set(multiValueArgs SOURCES ARGS)
    cmake_parse_arguments(PARSE_ARGV 0 arg
        "${options}" "${oneValueArgs}" "${multiValueArgs}"
    )

    add_executable(${arg_NAME} ${arg_SOURCES})
    target_link_libraries(${arg_NAME} ${JUNGLE_TEST_DEPS})
    if(NOT arg_SKIP)
        add_test(NAME ${arg_NAME} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} COMMAND ${arg_NAME} --abort-on-failure ${arg_ARGS})
        if(arg_TIMEOUT)
            set_tests_properties(${arg_NAME} PROPERTIES TIMEOUT ${arg_TIMEOUT})
        endif()
    endif()
endfunction()

unit_test(NAME fileops_test SOURCES ${TEST_DIR}/unit/fileops_test.cc)

unit_test(NAME fileops_directio_test SOURCES ${TEST_DIR}/unit/fileops_directio_test.cc)

unit_test(NAME keyvalue_test SOURCES ${TEST_DIR}/unit/keyvalue_test.cc)

unit_test(NAME memtable_test SOURCES ${TEST_DIR}/unit/memtable_test.cc)

unit_test(NAME table_test SOURCES ${TEST_DIR}/unit/table_test.cc)

unit_test(NAME crc32_test SOURCES ${TEST_DIR}/unit/crc32_test.cc)

unit_test(NAME table_lookup_booster_test SOURCES ${TEST_DIR}/unit/table_lookup_booster_test.cc)

add_custom_target(unit_tests)
add_dependencies(unit_tests
    fileops_test
    fileops_directio_test
    keyvalue_test
    crc32_test)

unit_test(NAME basic_op_test SOURCES ${TEST_DIR}/jungle/basic_op_test.cc)

unit_test(NAME sync_and_flush_test SOURCES ${TEST_DIR}/jungle/sync_and_flush_test.cc)

unit_test(NAME nearest_search_test SOURCES ${TEST_DIR}/jungle/nearest_search_test.cc)

unit_test(NAME disabling_seq_index_test SOURCES ${TEST_DIR}/jungle/disabling_seq_index_test.cc)

unit_test(NAME builder_test SOURCES ${TEST_DIR}/jungle/builder_test.cc)

unit_test(NAME seq_itr_test SOURCES ${TEST_DIR}/jungle/seq_itr_test.cc)

unit_test(NAME key_itr_test SOURCES ${TEST_DIR}/jungle/key_itr_test.cc)

unit_test(NAME snapshot_test SOURCES ${TEST_DIR}/jungle/snapshot_test.cc)

unit_test(NAME custom_cmp_test SOURCES ${TEST_DIR}/jungle/custom_cmp_test.cc)

unit_test(NAME corruption_test SOURCES ${TEST_DIR}/jungle/corruption_test.cc)

unit_test(NAME compaction_test SOURCES ${TEST_DIR}/jungle/compaction_test.cc)

unit_test(NAME level_extension_test SOURCES ${TEST_DIR}/jungle/level_extension_test.cc)

unit_test(NAME log_reclaim_test SOURCES ${TEST_DIR}/jungle/log_reclaim_test.cc)

unit_test(NAME custom_mani_verifier SOURCES ${TEST_DIR}/jungle/custom_mani_verifier.cc SKIP)

unit_test(NAME compression_test SOURCES ${TEST_DIR}/jungle/compression_test.cc)

unit_test(NAME mt_test SOURCES ${TEST_DIR}/jungle/mt_test.cc)

unit_test(NAME large_test SOURCES ${TEST_DIR}/jungle/large_test.cc)

unit_test(NAME atomic_batch_test SOURCES ${TEST_DIR}/jungle/atomic_batch_test.cc)

add_custom_target(func_test)
add_dependencies(func_test
    basic_op_test
    seq_itr_test
    key_itr_test
    snapshot_test
    custom_cmp_test
    mt_test
    large_test
    atomic_batch_test)


unit_test(NAME flush_stress_test SOURCES ${TEST_DIR}/stress/flush_stress_test.cc)

unit_test(NAME purge_stress_test SOURCES ${TEST_DIR}/stress/purge_stress_test.cc)

unit_test(NAME iterator_stress_test SOURCES ${TEST_DIR}/stress/iterator_stress_test.cc)

unit_test(NAME compactor_stress_test SOURCES ${TEST_DIR}/stress/compactor_stress_test.cc)

unit_test(NAME log_reclaim_stress_test SOURCES ${TEST_DIR}/stress/log_reclaim_stress_test.cc)

unit_test(NAME many_log_files_test SOURCES ${TEST_DIR}/stress/many_log_files_test.cc)

add_custom_target(stress_test)
add_dependencies(stress_test
    flush_stress_test
    purge_stress_test
    many_log_files_test)


unit_test(NAME basic_robust_child SOURCES ${TEST_DIR}/robust/basic_robust_child.cc)

unit_test(NAME dist_test SOURCES ${TEST_DIR}/bench/dist_def_test.cc)


# --- Benchmark ---

set(BENCH
    ${TEST_DIR}/bench/bench.cc
    ${TEST_DIR}/bench/db_adapter_jungle.cc)
add_executable(jungle_bench ${BENCH})
set_target_properties(jungle_bench
    PROPERTIES COMPILE_FLAGS
    "-DJUNGLE_ADAPTER=1")
target_link_libraries(jungle_bench ${JUNGLE_TEST_DEPS})



install(TARGETS
    basic_robust_child
    custom_mani_verifier
    jungle_bench
    RUNTIME DESTINATION bin
)


