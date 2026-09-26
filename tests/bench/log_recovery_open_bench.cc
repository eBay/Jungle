/************************************************************************
Copyright 2026 eBay Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

/*
 * Estimate the startup cost of always scanning the last log file in a
 * log-section store with lazy memtable loading. A manifest can have a valid
 * checksum yet lag the physical log if a crash occurs after the log file is
 * fsynced but before the updated sequence is persisted in the manifest.
 * Trusting that stale manifest can hide physical records and permit
 * overlapping sequence numbers on subsequent writes. Scanning the log tail
 * reconciles those two sources before recovery accepts new writes.
 *
 * An intact, single-log (~24 MiB) fixture is opened with
 * truncateInconsecutiveLogs enabled (tail scan) and disabled (control).
 * Comparing DB::open latency and peak RSS shows the normal startup cost of
 * the more robust scan, not the cost of repairing a damaged log. Disabling
 * truncateInconsecutiveLogs also disables other recovery checks, so the
 * no-scan result is only a control for an intact fixture.
 *
 * One local run on 2026-09-24: with an Apple M1 Max
 * (10 CPU cores, 64 GiB RAM), macOS 26.4.1/arm64, Apple clang 17,
 * CMake Debug build; direct I/O requested with a 32 KiB buffer.
 *   records=3072 value_bytes=8192 log_bytes=25288704 (~24.1 MiB)
 *   scan=1 open_ms=94.646 peak_rss_bytes=65110016 (~62.1 MiB)
 *   scan=0 open_ms=0.794 peak_rss_bytes=11157504 (~10.6 MiB)
 */

#include <libjungle/jungle.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <system_error>

#ifndef JUNGLE_RECOVERY_BENCH_DIR
#error "log_recovery_open_bench must be built with CMake"
#endif

#if defined(__APPLE__) || defined(__linux__)
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {

namespace fs = std::filesystem;

constexpr uint64_t NUM_RECORDS = 3072;
constexpr size_t VALUE_BYTES = 8 * 1024;
constexpr char FIXTURE_MARKER[] = "Jungle log recovery open benchmark v1";
constexpr char MARKER_FILENAME[] = "log_recovery_open_bench.fixture";

struct OpenSample {
    double elapsed_ms = 0;
    bool rss_available = false;
    uint64_t peak_rss_bytes = 0;
};

bool check_status(jungle::Status status, const char* operation) {
    if (status) return true;
    std::cerr << operation << " failed: " << status.toString() << '\n';
    return false;
}

jungle::DBConfig make_db_config(bool scan) {
    jungle::DBConfig config;
    config.logSectionOnly = true;
    config.logFileTtl_sec = 600;
    config.allowOverwriteSeqNum = true;
    config.maxEntriesInLogFile = 256 * 1024;
    config.maxLogFileSize = 64 * 1024 * 1024;
    config.directIoOpt.enabled = true;
    config.directIoOpt.bufferSize = 32 * 1024;
    config.truncateInconsecutiveLogs = scan;
    return config;
}

bool check_new_path(const fs::path& path) {
    std::error_code error;
    const auto status = fs::symlink_status(path, error);
    if (error && error != std::errc::no_such_file_or_directory) {
        std::cerr << "cannot inspect " << path << ": " << error.message() << '\n';
        return false;
    }
    if (status.type() != fs::file_type::not_found) {
        std::cerr << "fixture path already exists: " << path << '\n';
        return false;
    }
    return true;
}

bool make_default_fixture_path(fs::path& path) {
    std::error_code error;
    const fs::path base = JUNGLE_RECOVERY_BENCH_DIR;
    if (!fs::is_directory(base, error)) {
        std::cerr << "benchmark directory is not available: " << base;
        if (error) std::cerr << ": " << error.message();
        std::cerr << '\n';
        return false;
    }

    const std::string prefix = "log_recovery_open_bench_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    for (unsigned int attempt = 0; attempt < 100; ++attempt) {
        const fs::path parent =
            base / (prefix + "_" + std::to_string(attempt));
        if (fs::create_directory(parent, error)) {
            path = parent / "db";
            return true;
        }
        if (error && error != std::errc::file_exists) {
            std::cerr << "cannot create fixture directory " << parent << ": "
                      << error.message() << '\n';
            return false;
        }
        error.clear();
    }
    std::cerr << "cannot create a unique fixture directory in " << base << '\n';
    return false;
}

bool create_fixture(const fs::path& path) {
    jungle::DB* db = nullptr;
    if (!check_status(jungle::DB::open(&db, path.string(), make_db_config(true)),
                      "DB::open")) {
        return false;
    }

    bool success = true;
    const std::string value(VALUE_BYTES, 'v');
    for (uint64_t seq = 1; seq <= NUM_RECORDS; ++seq) {
        if (!check_status(db->setSN(seq, jungle::KV(std::to_string(seq), value)),
                          "DB::setSN")) {
            success = false;
            break;
        }
    }
    if (success) success = check_status(db->sync(true), "DB::sync");
    const bool closed = check_status(jungle::DB::close(db), "DB::close");
    return success && closed;
}

bool check_fixture(const fs::path& path, uintmax_t& log_bytes) {
    std::ifstream marker(path / MARKER_FILENAME);
    std::string contents;
    if (!std::getline(marker, contents) || contents != FIXTURE_MARKER) {
        std::cerr << "not a log recovery benchmark fixture: " << path << '\n';
        return false;
    }

    std::error_code error;
    if (!fs::is_regular_file(path / "log0000_manifest", error)) {
        std::cerr << "missing log manifest in " << path;
        if (error) std::cerr << ": " << error.message();
        std::cerr << '\n';
        return false;
    }
    log_bytes = fs::file_size(path / "log0000_00000000", error);
    if (error || !log_bytes) {
        std::cerr << "missing or empty fixture log in " << path;
        if (error) std::cerr << ": " << error.message();
        std::cerr << '\n';
        return false;
    }
    return true;
}

bool open_fixture(const fs::path& path, bool scan, OpenSample& sample) {
    jungle::DB* db = nullptr;
    const auto start = std::chrono::steady_clock::now();
    const jungle::Status status =
        jungle::DB::open(&db, path.string(), make_db_config(scan));
    const auto end = std::chrono::steady_clock::now();
    if (!check_status(status, "DB::open")) return false;
    sample.elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();

#if defined(__APPLE__) || defined(__linux__)
    bool rss_ok = true;
    struct rusage usage {};
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        sample.rss_available = true;
        sample.peak_rss_bytes = static_cast<uint64_t>(usage.ru_maxrss);
#ifdef __linux__
        sample.peak_rss_bytes *= 1024; // Linux reports KiB; macOS reports bytes.
#endif
    } else {
        std::cerr << "getrusage failed: " << std::strerror(errno) << '\n';
        rss_ok = false;
    }
#endif

    uint64_t max_seq = 0;
    bool valid = check_status(db->getMaxSeqNum(max_seq), "DB::getMaxSeqNum");
    if (valid && max_seq != NUM_RECORDS) {
        std::cerr << "unexpected maximum sequence: " << max_seq
                  << " (expected " << NUM_RECORDS << ")\n";
        valid = false;
    }
    const bool closed = check_status(jungle::DB::close(db), "DB::close");
#if defined(__APPLE__) || defined(__linux__)
    return valid && rss_ok && closed;
#else
    return valid && closed;
#endif
}

#if defined(__APPLE__) || defined(__linux__)
enum class Phase { CREATE, SCAN, NO_SCAN };

const char* phase_name(Phase phase) {
    switch (phase) {
    case Phase::CREATE: return "create";
    case Phase::SCAN: return "scan";
    case Phase::NO_SCAN: return "no-scan";
    }
    return "unknown";
}

bool execute_phase(const fs::path& path, Phase phase) {
    const bool create = phase == Phase::CREATE;
    uintmax_t log_bytes = 0;
    if (create) {
        if (!check_new_path(path)) return false;
    } else if (!check_fixture(path, log_bytes)) {
        return false;
    }

    jungle::GlobalConfig global;
    global.numFlusherThreads = 0;
    global.numCompactorThreads = 0;
    global.numTableWriters = 0;
    global.globalLogPath = path.parent_path().string();
    if (!check_status(jungle::init(global), "jungle::init")) return false;

    OpenSample sample;
    const bool success = create
        ? create_fixture(path)
        : open_fixture(path, phase == Phase::SCAN, sample);
    const bool shut_down = check_status(jungle::shutdown(), "jungle::shutdown");
    if (!success || !shut_down) return false;

    if (create) {
        std::ofstream marker(path / MARKER_FILENAME);
        marker << FIXTURE_MARKER << '\n';
        marker.close();
        if (!marker) {
            std::cerr << "cannot write fixture marker in " << path << '\n';
            return false;
        }
        std::error_code error;
        log_bytes = fs::file_size(path / "log0000_00000000", error);
        if (error) {
            std::cerr << "cannot read fixture log: " << error.message() << '\n';
            return false;
        }
        std::cout << "created=" << path << " records=" << NUM_RECORDS
                  << " value_bytes=" << VALUE_BYTES
                  << " log_bytes=" << log_bytes << '\n';
    } else {
        std::cout << "scan=" << (phase == Phase::SCAN)
                  << " log_bytes=" << log_bytes << " open_ms="
                  << std::fixed << std::setprecision(3) << sample.elapsed_ms
                  << " peak_rss_bytes=";
        if (sample.rss_available) {
            std::cout << sample.peak_rss_bytes;
        } else {
            std::cout << "unavailable";
        }
        std::cout << '\n';
    }
    std::cout.flush();
    return std::cout.good();
}

bool run_phase(const fs::path& path, Phase phase) {
    // ru_maxrss is a lifetime high-water mark; use a separate process
    // per phase to measure peak RSS independently.
    const pid_t child = fork();
    if (child == -1) {
        std::cerr << "cannot fork " << phase_name(phase) << ": "
                  << std::strerror(errno) << '\n';
        return false;
    }
    if (child == 0) {
        const bool success = execute_phase(path, phase);
        std::cerr.flush();
        _exit(success ? 0 : 1);
    }

    int status = 0;
    pid_t waited;
    do {
        waited = waitpid(child, &status, 0);
    } while (waited == -1 && errno == EINTR);
    if (waited == -1) {
        std::cerr << "waitpid failed for " << phase_name(phase) << ": "
                  << std::strerror(errno) << '\n';
        return false;
    }
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) return true;

    std::cerr << phase_name(phase) << " failed";
    if (WIFEXITED(status)) {
        std::cerr << " with exit code " << WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        std::cerr << " with signal " << WTERMSIG(status);
    }
    std::cerr << '\n';
    return false;
}
#endif

} // namespace

int main(int argc, char** argv) {
    if (argc != 1) {
        std::cerr << "Usage: " << argv[0] << " (no arguments)\n";
        return 2;
    }
#if defined(__APPLE__) || defined(__linux__)
    fs::path path;
    if (!make_default_fixture_path(path)) return 1;
    const bool success = run_phase(path, Phase::CREATE) &&
                         run_phase(path, Phase::SCAN) &&
                         run_phase(path, Phase::NO_SCAN);

    std::error_code error;
    fs::remove_all(path.parent_path(), error);
    if (error) {
        std::cerr << "cannot remove fixture directory "
                  << path.parent_path() << ": " << error.message() << '\n';
        return 1;
    }
    return success ? 0 : 1;
#else
    std::cerr << "this benchmark requires POSIX process support\n";
    return 1;
#endif
}
