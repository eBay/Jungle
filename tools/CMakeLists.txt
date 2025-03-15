set(TOOLS_DIR ${PROJECT_SOURCE_DIR}/tools)

set(JUNGLE_TOOLS_DEPS
    static_lib
    ${LIBSIMPLELOGGER}
    ${FORESTDB}
    ${LIBSNAPPY}
    ${LIBDL})

set(BF_GEN ${TOOLS_DIR}/bloomfilter_generator.cc)
add_executable(bloomfilter_generator ${BF_GEN})
target_link_libraries(bloomfilter_generator ${JUNGLE_TOOLS_DEPS})
add_dependencies(bloomfilter_generator static_lib)

set(JUNGLE_CHECKER ${TOOLS_DIR}/jungle_checker.cc)
add_executable(jungle_checker ${JUNGLE_CHECKER})
target_link_libraries(jungle_checker ${JUNGLE_TOOLS_DEPS})
add_dependencies(jungle_checker static_lib)

install(TARGETS
    bloomfilter_generator jungle_checker
    RUNTIME DESTINATION bin
)
