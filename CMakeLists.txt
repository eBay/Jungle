cmake_minimum_required(VERSION 3.5)
project(Jungle VERSION 1.0.0 LANGUAGES CXX)

# === Build type (default: RelWithDebInfo, O2) ===========
if (NOT CMAKE_BUILD_TYPE)
    set(BUILD_TYPE_OPTIONS
        "Choose the type of build, "
        "options are: Debug Release RelWithDebInfo MinSizeRel.")
    set(CMAKE_BUILD_TYPE "RelWithDebInfo"
        CACHE ${BUILD_TYPE_OPTIONS} FORCE)
    message(STATUS "Build type is not given, use default.")
endif ()
message(STATUS "Build type: " ${CMAKE_BUILD_TYPE})


set(ROOT_SRC ${PROJECT_SOURCE_DIR}/src)
set(CMAKE_DIR ${PROJECT_SOURCE_DIR}/cmake)
set(FDB_LIB_DIR ${PROJECT_SOURCE_DIR}/third_party/forestdb/build)

if (CODE_COVERAGE GREATER 0)
    set(CMAKE_BUILD_TYPE "Debug")
    include(cmake/CodeCoverage.cmake)
    message(STATUS "---- CODE COVERAGE DETECTION MODE ----")
endif()

# Libraries
#set(LIBZ z)

# Find snappy
include(cmake/FindSnappy.cmake)
if ( (SNAPPY_OPTION STREQUAL "Enable") OR
     (SNAPPY_OPTION STREQUAL "enable") )
    message(STATUS "Snappy option is enabled.")
    if (SNAPPY_FOUND)
        set(LIBSNAPPY ${SNAPPY_LIBRARIES})
        add_definitions(-DSNAPPY_AVAILABLE=1)
    else()
        MESSAGE(FATAL_ERROR "Can't find snappy, "
                "if you want to build without snappy set"
                " \"-DSNAPPY_OPTION=Disable\"")
    endif(SNAPPY_FOUND)
endif()

set(LIBDL dl)

# Includes
include_directories(BEFORE ./)
include_directories(BEFORE ${PROJECT_SOURCE_DIR}/src)
include_directories(BEFORE ${PROJECT_SOURCE_DIR}/include)
include_directories(BEFORE ${PROJECT_SOURCE_DIR}/tools)
include_directories(BEFORE ${PROJECT_SOURCE_DIR}/tests)

# Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-pessimizing-move")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
if (NOT APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif ()

if (CODE_COVERAGE GREATER 0)
    APPEND_COVERAGE_COMPILER_FLAGS()
    #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")
    #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-inline")
    #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_NO_EXCEPTION")
    set(COVERAGE_EXCLUDES
        '*third_party/*'
        '*usr/*'
        '*tests/*'
        '*tools/*'
        '*src/simple_thread_pool.h'
        '*src/logger.*'
    )
endif()


# === SANITIZER ===

if (ADDRESS_SANITIZER GREATER 0)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fuse-ld=gold")
    message(STATUS "---- ADDRESS SANITIZER IS ON ----")
endif()

if (THREAD_SANITIZER GREATER 0)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
    message(STATUS "---- THREAD SANITIZER IS ON ----")
endif()

if (LEAK_SANITIZER GREATER 0)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=leak")
    message(STATUS "---- LEAK SANITIZER IS ON ----")
endif()



# === Program flags ===

if (TESTSUITE_NO_COLOR GREATER 0)
    add_definitions(-DTESTSUITE_NO_COLOR=1)
    message(STATUS "---- MONOCOLOR TESTSUITE ----")
endif()

if (LOGGER_NO_BACKTRACE GREATER 0)
    add_definitions(-DLOGGER_NO_BACKTRACE=1)
    message(STATUS "---- NO BACKTRACE BY LOGGER ----")
endif()

file(COPY ${PROJECT_SOURCE_DIR}/scripts/runtests.sh
     DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

# === CUSTOM LOGGER ===

if (LOGGER_PATH)
    set(LOGGER_CC_FILE "${LOGGER_PATH}/logger.cc")
    set(LOGGER_HEADER_FILE "${LOGGER_PATH}/logger.h")
elseif (NOT LOGGER_CC_FILE)
    set(LOGGER_CC_FILE "${ROOT_SRC}/logger.cc")
    set(LOGGER_HEADER_FILE "${ROOT_SRC}/logger.h")
endif ()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DLOGGER_H=${LOGGER_HEADER_FILE}")
message(STATUS "Simple logger cc file: ${LOGGER_CC_FILE}")
message(STATUS "Simple logger header file: ${LOGGER_HEADER_FILE}")


# === Source files ===================
set(LOGGER_SRC ${LOGGER_CC_FILE})

if (DETACH_LOGGER GREATER 0)
    message(STATUS "---- DETACH LOGGER ----")
    set(LIBSIMPLELOGGER "${CMAKE_CURRENT_BINARY_DIR}/libsimplelogger.a")

    add_library(simplelogger_lib ${LOGGER_SRC})
    set_target_properties(simplelogger_lib PROPERTIES OUTPUT_NAME simplelogger
                          CLEAN_DIRECT_OUTPUT 1)

else ()
    set(LOGGER_SRC_TO_CORE ${LOGGER_SRC})

endif ()

set(JUNGLE_CORE
    ${ROOT_SRC}/avltree.cc
    ${ROOT_SRC}/bloomfilter.cc
    ${ROOT_SRC}/cmd_handler.cc
    ${ROOT_SRC}/compactor.cc
    ${ROOT_SRC}/crc32.cc
    ${ROOT_SRC}/db_config.cc
    ${ROOT_SRC}/db_group.cc
    ${ROOT_SRC}/db_manifest.cc
    ${ROOT_SRC}/db_mgr.cc
    ${ROOT_SRC}/fileops_posix.cc
    ${ROOT_SRC}/fileops_directio.cc
    ${ROOT_SRC}/flusher.cc
    ${ROOT_SRC}/global_batch.cc
    ${ROOT_SRC}/internal_helper.cc
    ${ROOT_SRC}/iterator.cc
    ${ROOT_SRC}/jungle.cc
    ${ROOT_SRC}/jungle_builder.cc
    ${ROOT_SRC}/log_file.cc
    ${ROOT_SRC}/log_iterator.cc
    ${ROOT_SRC}/log_manifest.cc
    ${ROOT_SRC}/log_mgr.cc
    ${ROOT_SRC}/log_reclaimer.cc
    ${ROOT_SRC}/memtable.cc
    ${ROOT_SRC}/memtable_iterator.cc
    ${ROOT_SRC}/murmurhash3.cc
    ${ROOT_SRC}/sampler.cc
    ${ROOT_SRC}/skiplist.cc
    ${ROOT_SRC}/table_append.cc
    ${ROOT_SRC}/table_compact_condition.cc
    ${ROOT_SRC}/table_compaction.cc
    ${ROOT_SRC}/table_file.cc
    ${ROOT_SRC}/table_file_compaction.cc
    ${ROOT_SRC}/table_file_iterator.cc
    ${ROOT_SRC}/table_helper.cc
    ${ROOT_SRC}/table_iterator.cc
    ${ROOT_SRC}/table_lookup_booster.cc
    ${ROOT_SRC}/table_manifest.cc
    ${ROOT_SRC}/table_mgr.cc
    ${ROOT_SRC}/table_set_batch.cc
    ${ROOT_SRC}/table_split.cc
    ${ROOT_SRC}/table_writer.cc
    ${ROOT_SRC}/worker_mgr.cc
    ${LOGGER_SRC_TO_CORE}
    ${OPEN_MEMSTREAM}
)
#add_library(JUNGLE_CORE_OBJ OBJECT ${JUNGLE_CORE})

# Note: static libraries MUST be located in front of all shared libraries.
set(JUNGLE_DEPS
    ${FDB_LIB_DIR}/libforestdb.a
    ${LIBSNAPPY}
    ${LIBDL})

add_library(static_lib ${JUNGLE_CORE})
target_include_directories(static_lib PUBLIC "${PROJECT_SOURCE_DIR}/include")
target_link_libraries(static_lib ${JUNGLE_DEPS})
set_target_properties(static_lib PROPERTIES OUTPUT_NAME jungle
                      CLEAN_DIRECT_OUTPUT 1)
if (DETACH_LOGGER GREATER 0)
    add_dependencies(static_lib simplelogger_lib)
endif ()

add_subdirectory("${PROJECT_SOURCE_DIR}/examples")
add_subdirectory("${PROJECT_SOURCE_DIR}/tests")
add_subdirectory("${PROJECT_SOURCE_DIR}/tools")

if (CODE_COVERAGE GREATER 0)
    SETUP_TARGET_FOR_COVERAGE(
        NAME jungle_cov
        EXECUTABLE ./runtests.sh
        DEPENDENCIES keyvalue_test
                     crc32_test
                     fileops_test
                     fileops_directio_test
                     memtable_test
                     table_test
                     basic_op_test
                     sync_and_flush_test
                     nearest_search_test
                     seq_itr_test
                     key_itr_test
                     snapshot_test
                     custom_cmp_test
                     corruption_test
                     compaction_test
                     mt_test
                     log_reclaim_test
                     custom_mani_verifier
                     level_extension_test
                     table_lookup_booster_test
                     compression_test
                     atomic_batch_test
                     builder_test
    )
endif()
