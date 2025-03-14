set(EXAMPLES_DIR ${PROJECT_SOURCE_DIR}/examples)

set(JUNGLE_TEST_DEPS
    static_lib
    ${LIBSIMPLELOGGER}
    ${FORESTDB}
    ${LIBSNAPPY}
    ${LIBDL})

set(GET_SET_DEL ${EXAMPLES_DIR}/example_get_set_del.cc)
add_executable(get_set_del ${GET_SET_DEL})
target_link_libraries(get_set_del ${JUNGLE_TEST_DEPS})
add_dependencies(get_set_del static_lib)

set(ITERATOR ${EXAMPLES_DIR}/example_iterator.cc)
add_executable(iterator ${ITERATOR})
target_link_libraries(iterator ${JUNGLE_TEST_DEPS})
add_dependencies(iterator static_lib)

set(ITERATOR_ADV ${EXAMPLES_DIR}/example_iterator_adv.cc)
add_executable(iterator_adv ${ITERATOR_ADV})
target_link_libraries(iterator_adv ${JUNGLE_TEST_DEPS})
add_dependencies(iterator_adv static_lib)

set(SNAPSHOT_CHK ${EXAMPLES_DIR}/example_snapshot_checkpoint.cc)
add_executable(snapshot_checkpoint ${SNAPSHOT_CHK})
target_link_libraries(snapshot_checkpoint ${JUNGLE_TEST_DEPS})
add_dependencies(snapshot_checkpoint static_lib)

set(SNAPSHOT_INST ${EXAMPLES_DIR}/example_snapshot_instant.cc)
add_executable(snapshot_instant ${SNAPSHOT_INST})
target_link_libraries(snapshot_instant ${JUNGLE_TEST_DEPS})
add_dependencies(snapshot_instant static_lib)

set(LOG_STORE ${EXAMPLES_DIR}/example_log_store_mode.cc)
add_executable(log_store ${LOG_STORE})
target_link_libraries(log_store ${JUNGLE_TEST_DEPS})
add_dependencies(log_store static_lib)

install(TARGETS
    get_set_del iterator iterator_adv snapshot_checkpoint snapshot_instant
    log_store
    RUNTIME DESTINATION bin
)
