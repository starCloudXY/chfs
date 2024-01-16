#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include "rsm/list_state_machine.h"
#include <mutex>
#include <vector>
#include <cstring>
#include <random>
#define  RAFT_LOG_SIZE 64*1024
#define SINGLE_LOG_SIZE 4*1024
namespace chfs {
    constexpr char check_pin = '#';
    using term_id_t = int;
    using node_id_t = int;
    using commit_id_t = unsigned long;

    template<typename Command>
    struct LogEntry {
        term_id_t term_id;
        Command command;

        LogEntry(term_id_t term, Command cmd)
                : term_id(term), command(cmd) {

        }

        LogEntry(): term_id(0) {
            command = ListCommand(0);
        }
    };

/**
 * RaftLog uses a BlockManager to manage the data..
 */
    template<typename Command>
    class RaftLog {
    public:
        RaftLog(std::shared_ptr<BlockManager> bm);

        ~RaftLog();

        explicit RaftLog(node_id_t nodeId,
                         const std::shared_ptr<BlockManager> &bm);

        /* Lab3: Your code here */
        [[nodiscard]] int get_last_include_idx() const {

            return last_included_idx; }


        void insert_or_rewrite(LogEntry<Command> &entry, int idx);

        std::pair<term_id_t, commit_id_t> get_last();

        size_t size();

        //add a new log
        commit_id_t append_log(term_id_t term, Command command);

        void insert(term_id_t term, Command command);

        void save(const term_id_t current_term, const int vote_for);

        std::pair<term_id_t,commit_id_t> recover();

        LogEntry<Command> get_nth(int n);

        std::vector<LogEntry<Command>> get_after_nth(int n);

        void delete_after_nth(int n);

        void write_snapshot(
                const int offset, std::vector<u8> data);
        void last_snapshot(
                term_id_t last_included_term, int last_included_idx);
        void delete_before_nth(int n);
        std::pair<int, std::vector<int>> get_snapshot_data() const;
        [[nodiscard]] term_id_t get_last_include_term() const {
            return last_included_term;
        }
        std::vector<u8> create_snap(const int commit_idx);
    private:
        std::shared_ptr<BlockManager> bm_;
        std::mutex mtx;
        std::shared_ptr<BlockManager> meta_bm_, data_bm_, snap_bm_;
        /* Lab3: Your code here */
        std::vector<LogEntry<Command>> logs;
        inode_id_t last_included_idx = 0;
        term_id_t last_included_term = 0;
        inode_id_t snapshot_idx = 0;
        node_id_t node_id;


        void write_meta(const term_id_t current_term, const int vote_for);

        void write_data();

        std::tuple<term_id_t, int, int> get_meta();

        void get_data(const int size);

        int per_entry_size = 0;


        /* snapshot */


    };
    template <typename Command>
    RaftLog<Command>::RaftLog(const node_id_t nodeId,
                              const std::shared_ptr<BlockManager>& bm)
            : node_id(nodeId) {
        bm_ = bm;
        meta_bm_ = std::make_shared<BlockManager>("/tmp/raft_log/meta_" +
                                                      std::to_string(node_id));
        data_bm_ = std::make_shared<BlockManager>(
                "/tmp/raft_log/data_" + std::to_string(node_id));
        logs.emplace_back(LogEntry<Command>());
    }

    template<typename Command>
    RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) {
        /* Lab3: Your code here */
        bm_ = bm;
        logs.emplace_back(LogEntry<Command>());
    }

    template<typename Command>
    RaftLog<Command>::~RaftLog() {
        /* Lab3: Your code here */
    }

    template<typename Command>
    void RaftLog<Command>::insert_or_rewrite(LogEntry<Command> &entry, int idx) {
        std::lock_guard lockGuard(mtx);
        while (logs.size() - 1 < idx) {
            logs.emplace_back(LogEntry<Command>());
        }
        logs[idx] = entry;
    }
    template <typename Command>
    void RaftLog<Command>::delete_after_nth(int n) {
        if (logs.size() <= n) return;
        // delete after nth and include nth
        auto begin_ = logs.begin() + n;
        logs.erase(begin_, logs.end());
    }
    ////
    template<typename Command>
    void RaftLog<Command>::save(const term_id_t current_term, const int vote_for) {
        std::lock_guard lockGuard(mtx);
        write_meta(current_term, vote_for);
        write_data();
    }

    template <typename Command>
    std::pair<int, std::vector<int>> RaftLog<Command>::get_snapshot_data() const {
        if (snap_bm_ == nullptr) return {0, {}};
        std::vector<u8> buffer(snap_bm_->block_size());
        std::vector<int> ret_val;
        std::stringstream ss, ret_ss;
        std::string str;
        int block_idx = 0;
        int size = 0, tmp;
        snap_bm_->read_block(block_idx++, buffer.data());
        str.assign(buffer.begin(), buffer.end());
        ss.str(str);
        ss >> size;
        ret_val.reserve(size);
        for (int i = 0; i < size; ++i) {
            if (ss.eof()) {
                snap_bm_->read_block(block_idx++, buffer.data());
                str.assign(buffer.begin(), buffer.end());
                ss.str(str);
            }
            ss >> tmp;
            ret_val.emplace_back(tmp);
        }
        // LOG_FORMAT_INFO("size: {}, val_size: {}", size, ret_val.size());
        return {size, ret_val};
    }


    template<typename Command>
    std::pair<term_id_t,commit_id_t> RaftLog<Command>::recover() {
        std::lock_guard lockGuard(mtx);
        logs.clear();
        auto [cur_term, vote_for, size] = get_meta();
        if (size == 0) {
            return {cur_term, vote_for};
        }
        get_data(size);
        return {cur_term, vote_for};
    }
    template <typename Command>
    std::vector<u8> RaftLog<Command>::create_snap(const int commit_idx) {
        std::lock_guard lockGuard(mtx);
        auto [size, value] = get_snapshot_data();
        int i = 1;
        if (size == 0) {
            // no snapshot before
            size = 1;
            i = 0;
        }
        const auto idx = commit_idx - last_included_idx;
        size += idx;
        for (; i <= idx; ++i) {
            if (i > logs.size()) break;
            int val = logs[i].command.value;
            value.emplace_back(val);
        }
        std::stringstream ss;
        ss << size;
        for (const auto v : value) {
            ss << ' ' << v;
        }
        std::string str = ss.str();
        std::vector<u8> data(str.begin(), str.end());
        return data;
    }
    template<typename Command>
    void RaftLog<Command>::write_meta(const term_id_t current_term,
                                      const int vote_for) {
        std::stringstream ss;
        ss <<check_pin<<' '
            << current_term << ' '
           << vote_for << ' '
           << (int) (logs.size()) << ' '
           << snapshot_idx << ' '
           << last_included_idx << ' '
           << last_included_term;
        std::string str = ss.str();
        const std::vector<u8> data(str.begin(), str.end());
        std::cout<<"[WRITE META] "<<str<<"\n";
        meta_bm_->write_partial_block(0, data.data(), 0, data.size());
    }

    template<typename Command>
    std::tuple<term_id_t, int, int> RaftLog<Command>::get_meta() {
        std::vector<u8> buffer(meta_bm_->block_size());
        meta_bm_->read_block(0, buffer.data());
        std::string str;
        str.assign(buffer.begin(), buffer.end());
        std::stringstream ss(str);
        term_id_t current_term;
        int vote_for;
        int size;
        char check;
        ss >>check;
        if (check != check_pin) {
            logs.emplace_back(LogEntry<Command>());
            return {0, -1, 0};
        }
        ss >> current_term
           >> vote_for
           >> size
           >> snapshot_idx
           >> last_included_idx
           >> last_included_term;
        snap_bm_ = std::make_shared<BlockManager>("/tmp/raft_log/snap_" +
                                                  std::to_string(node_id) + "_" +
                                                  std::to_string(snapshot_idx));
        return {current_term, vote_for, size};
    }

    template<typename Command>
    commit_id_t RaftLog<Command>::append_log(term_id_t term, Command command) {
        std::lock_guard lockGuard(mtx);
        logs.emplace_back(LogEntry<Command>(term, command));
        return logs.size() - 1;
    }
    ///TODO
    template <typename Command>
    void RaftLog<Command>::write_snapshot(
            const int offset, std::vector<u8> data) {
        std::lock_guard lockGuard(mtx);
        if (offset == 0) {
            // a new snapshot file
            snapshot_idx++;
            snap_bm_ = std::make_shared<BlockManager>("/tmp/raft_log/snap_" +
                                                      std::to_string(node_id) + "_" +
                                                      std::to_string(snapshot_idx));
        }
        assert(snap_bm_ != nullptr);
        // write data into snapshot file
        int block_idx = offset / snap_bm_->block_size();
        int block_offset = offset % snap_bm_->block_size();
        unsigned len = data.size();
        std::vector<u8> buffer(snap_bm_->block_size());
        while (len) {
            const unsigned write_len = std::min(
                    len, snap_bm_->block_size() - block_offset);
            std::copy_n(data.begin(), write_len, buffer.begin());
            snap_bm_->write_partial_block(block_idx++, buffer.data(), block_offset,
                                          write_len);
            // update data
            data.erase(data.begin(), data.begin() + write_len);
            len -= write_len;
            block_offset = 0;
        }
    }
    template <typename Command>
    void RaftLog<Command>::delete_before_nth(int n) {
        if (logs.size() <= n) return;
        // delete before nth and include nth
        auto begin_ = logs.begin();
        logs.erase(begin_ + 1, begin_ + n + 1);
    }
    template <typename Command>
    void RaftLog<Command>::last_snapshot(
            term_id_t last_term, int last_idx) {
        std::lock_guard lockGuard(mtx);
        // delete other snapshot files
        for (int i = 0; i < snapshot_idx; i++) {
            std::string path = "/tmp/raft_log/snap_" + std::to_string(node_id) + "_" +
                               std::to_string(i);
            std::remove(path.c_str());
        }
        // check the logs_ entry
        auto op_idx = last_idx - this->last_included_idx;
        if (logs.size() < op_idx) {
            logs.erase(logs.begin() + 1, logs.end());
            this->last_included_idx = last_idx;
            this->last_included_term = last_term;
            return;
        }
        if (auto [term, _] = logs[op_idx]; term == last_included_term) {
            delete_before_nth(op_idx);
        } else {
            logs.erase(logs.begin() + 1, logs.end());
        }
        assert(logs.size() > 0);
        this->last_included_idx = last_idx;
        this->last_included_term = last_term;
    }
    template <typename Command>
    void RaftLog<Command>::insert(term_id_t term, Command command) {
        std::lock_guard lockGuard(mtx);
        logs.insert(logs.begin() + 1, LogEntry<Command>(term, command));
    }

    template<typename Command>
    std::pair<term_id_t, commit_id_t> RaftLog<Command>::get_last() {
        std::lock_guard lockGuard(mtx);
        if (logs.size() <= 1) {
            return {
                last_included_term,
                last_included_idx};
        }
        return {logs.back().term_id,
                last_included_idx + logs.size() - 1};
}

    template <typename Command>
    size_t RaftLog<Command>::size() {
        std::lock_guard lockGuard(mtx);
        return logs.size();
    }
////
    template <typename Command>
    void RaftLog<Command>::write_data() {
        std::stringstream ss;
        int used_bytes = 0, block_idx = 0;
        bool first = true;
        for (auto &log: logs) {
            ss << log.term_id << ' '
               << (int) log.command.value << ' ';
            std::cout<<"[WRITE DATA]  "<<log.term_id<<' '<<(int)log.command.value<<std::endl;
            used_bytes += per_entry_size;
            if (first) {
                first = false;
                per_entry_size = ss.str().size();
                used_bytes = per_entry_size;
            }
            // data is full,then write in
            if (used_bytes + per_entry_size > bm_->block_size()) {
                std::string buffer = ss.str();
                const std::vector<u8> data(buffer.begin(), buffer.end());
                data_bm_->write_partial_block(
                        block_idx++,
                        data.data(),
                        0,
                        data.size()
                );
                ss.clear();
                used_bytes = 0;
            }
        }

        std::string str = ss.str();
        const std::vector<u8> data(str.begin(), str.end());
        data_bm_->write_partial_block(block_idx++, data.data(), 0,
                                      data.size());

    }
    ////?
    template <typename Command>
    std::vector<LogEntry<Command>> RaftLog<Command>::get_after_nth(int n) {
        if (logs.size() <= n) return {};
        auto begin_ = logs.begin() + n + 1;
        auto ret_val = std::vector<LogEntry<Command>>(begin_, logs.end());
        return ret_val;
    }
////
    template <typename Command>
    LogEntry<Command> RaftLog<Command>::get_nth(int n) {
        std::lock_guard lockGuard(mtx);
        if (n <= 0) {
            // invalid  command
            return LogEntry<Command>{
                    last_included_term, ListCommand(0)};
        }
        //        If there exists an N such that N > commitIndex, a majority
        //        of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        //        set commitIndex = N
        while (logs.size() - 1< n) {
            logs.emplace_back(LogEntry<Command>());
        }
        return logs[n];
    }
    ////
    template<typename Command>
    void RaftLog<Command>::get_data(const int size) {
        int block_idx = 0;
        int used_bytes = 0;
        std::vector<u8> buffer(data_bm_->block_size());
        data_bm_->read_block(block_idx++, buffer.data());
        std::string str;
        std::stringstream ss;
        int term, value;
        str.assign(buffer.begin(), buffer.end());
        ss.str(str);
        for (int i = 0; i < size; i++) {
            if (used_bytes + per_entry_size > bm_->block_size()) {
                data_bm_->read_block(block_idx++, buffer.data());
                str.assign(buffer.begin(), buffer.end());
                used_bytes = 0;
            }
            ss >> term >> value;
//            std::cout<<"get "<<term<<"  "<<value<<std::endl;
            logs.emplace_back(LogEntry<Command>(term, Command(value)));
            used_bytes += per_entry_size;
        }
    }

/* Lab3: Your code here */
    class Timer {
    public:
        explicit Timer(const long long interval) : interval(interval) {
            fixed = true;
            start_time = std::chrono::steady_clock::now();
            //use rd to generate random
            gen = std::mt19937(rd());
            dist = std::uniform_int_distribution(500, 550);
        }

        Timer() {
            start_time = std::chrono::steady_clock::now();
            gen = std::mt19937(rd());
            dist = std::uniform_int_distribution(600, 1000);
            interval = dist(gen);
        }

        void start() {
            started = true;
            start_time = std::chrono::steady_clock::now();
        }

        void stop() {
            started.store(false);
        };

        void reset() {
            start_time = std::chrono::steady_clock::now();
            receive_from_leader = false;
            if (fixed) {
                return;
            }
            interval = dist(gen);
        }

        void receive() {
            receive_from_leader.store(true);
        }

        bool check_receive() const {
            return receive_from_leader.load();
        }

        bool timeout() {
            if (started) {
                curr_time = std::chrono::steady_clock::now();
                if (const auto duration =
                            std::chrono::duration_cast<std::chrono::milliseconds>(
                                    curr_time - start_time)
                                    .count();
                        duration > interval) {
                    reset();
                    return true;
                }
                reset();
            }
            return false;
        };

        [[nodiscard]] std::chrono::milliseconds sleep_for() const {
            return std::chrono::milliseconds(interval);
        }

    private:
        bool fixed = false;
        long long interval;
        std::atomic<bool> started = false;
        std::atomic<bool> receive_from_leader = false;
        std::chrono::steady_clock::time_point start_time;
        std::chrono::steady_clock::time_point curr_time;
        std::random_device rd;
        std::mt19937 gen;
        std::uniform_int_distribution<int> dist;

    };

/* namespace chfs */
}