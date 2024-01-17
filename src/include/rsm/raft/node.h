#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"


namespace chfs {
    int ThreadNum = 3;
    enum class RaftRole {
        Follower,
        Candidate,
        Leader
    };

    struct RaftNodeConfig {
        int node_id;
        uint16_t port;
        std::string ip_address;
    };

    template <typename StateMachine, typename Command>
    class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

    public:
        RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);
        ~RaftNode();

        /* interfaces for test */
        void set_network(std::map<int, bool> &network_availablility);
        void set_reliable(bool flag);
        int get_list_state_log_num();
        int rpc_count();
        std::vector<u8> get_snapshot_direct();
        void recover_from_disk();

    private:
        /*
         * Start the raft node.
         * Please make sure all of the rpc request handlers have been registered before this method.
         */
        auto start() -> int;
        bool is_more_up_to_data(chfs::term_id_t termId,
                                int index);

        /*
         * Stop the raft node.
         */
        auto stop() -> int;

        /* Returns whether this node is the leader, you should also return the current term. */
        auto is_leader() -> std::tuple<bool, int>;

        /* Checks whether the node is stopped */
        auto is_stopped() -> bool;

        /*
         * Send a new command to the raft nodes.
         * The returned tuple of the method contains three values:
         * 1. bool:  True if this raft node is the leader that successfully appends the log,
         *      false If this node is not the leader.
         * 2. int: Current term.
         * 3. int: Log index.
         */
        auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

        /* Save a snapshot of the state machine and compact the log. */
        auto save_snapshot() -> bool;

        /* Get a snapshot of the state machine */
        auto get_snapshot() -> std::vector<u8>;


        /* Internal RPC handlers */
        auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
        auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
        auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

        /* RPC helpers */
        void send_request_vote(int target, RequestVoteArgs arg);
        void handle_request_vote_reply(int target, const RequestVoteArgs &arg, const RequestVoteReply reply);

        void send_append_entries(int target, AppendEntriesArgs<Command> arg);
        void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

        void send_install_snapshot(int target, InstallSnapshotArgs arg);
        void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

        /* background workers */
        void run_background_ping();
        void run_background_snapshot();
        void run_background_election();
        void run_background_commit();
        void run_background_apply();
        RequestVoteArgs create_request_vote();
        void become_follower(const term_id_t term,
                             const node_id_t leader);
        void become_candidate();
        void become_leader();
        std::pair<term_id_t, commit_id_t> get_last();
        int get_last_include_idx(){
            return log_storage->get_last_include_idx();
        };

        /* Data structures */
        bool network_stat;          /* for test */

        std::mutex mtx;                             /* A big lock to protect the whole data structure. */
        std::mutex clients_mtx;                     /* A lock to protect RpcClient pointers */
        std::unique_ptr<ThreadPool> thread_pool;
        std::unique_ptr<RaftLog<Command>> log_storage;     /* To persist the raft log. */
        std::unique_ptr<StateMachine> state;  /*  The state machine that applies the raft log, e.g. a kv store. */
        std::map<int, node_id_t> applied_map; /* The map from a log entry to applied num. */

        std::unique_ptr<RpcServer> rpc_server;      /* RPC server to recieve and handle the RPC requests. */
        std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map;  /* RPC clients of all raft nodes including this node. */
        std::vector<RaftNodeConfig> node_configs;   /* Configuration for all nodes */
        int my_id;                                  /* The index of this node in rpc_clients, start from 0. */

        std::atomic_bool stopped;

        RaftRole role;
        term_id_t current_term = 0;
        int leader_id;
        node_id_t vote_for;
//    for each server, index of the next log entry to send to that server (initialized to leader
//    last log index + 1)
        std::map<node_id_t, commit_id_t> next_index;
//    for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
        std::map<node_id_t, commit_id_t> match_index;
        int votes_received;
        commit_id_t commit_idx = 0;
        int last_applied;
        Timer fixed_timer{200};
        Timer timer{};
        std::vector<u8> installing_data;
        std::unique_ptr<std::thread> background_election;
        std::unique_ptr<std::thread> background_ping;
        std::unique_ptr<std::thread> background_commit;
        std::unique_ptr<std::thread> background_apply;
        std::unique_ptr<std::thread> background_snapshot;
        /* Lab3: Your code here */
    };

    template <typename StateMachine, typename Command>
    RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs):
            network_stat(true),
            node_configs(configs),
            my_id(node_id),
            stopped(true),
            role(RaftRole::Follower),
            current_term(0),
            leader_id(-1)
    {
        auto my_config = node_configs[my_id];

        /* launch RPC server */
        rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

        /* Register the RPCs. */
        rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
        rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
        rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
        rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
        rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
        rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
        rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

        rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
        rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
        rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

        /* Lab3: Your code here */
        thread_pool = std::make_unique<ThreadPool>(ThreadNum);
        auto bm = std::make_shared<BlockManager>("/tmp/raft_log/" +
                                                 std::to_string(node_id) +
                                                 ".log");
        log_storage = std::make_unique<RaftLog<Command>>(node_id, bm);
        state = std::make_unique<StateMachine>();
        rpc_server->run(true, configs.size());
    }

    template <typename StateMachine, typename Command>
    RaftNode<StateMachine, Command>::~RaftNode()
    {
        stop();
        thread_pool.reset();
        rpc_server.reset();
        state.reset();
        log_storage.reset();
    }

/******************************************************************

                        RPC Interfaces

*******************************************************************/


    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::start() -> int
    {
        /* Lab3: Your code here */
        //binding with port
        for (const auto& config : this->node_configs) {
            this->rpc_clients_map[config.node_id] =
                    std::make_unique<RpcClient>
                            (config.ip_address,
                             config.port,
                             true);
        }
        become_follower(0, -1);
        ////
        recover_from_disk();
        stopped = false;
        background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
        background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
        background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
        background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);
        background_snapshot = std::make_unique<std::thread>(&RaftNode::run_background_snapshot, this);
        return 0;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::stop() -> int
    {
        stopped.store(true);
        /* Lab3: Your code here */
        background_apply->join();
        background_commit->join();
        background_election->join();
        background_ping->join();
        background_snapshot->join();
        rpc_server->stop();
        return 0;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
    {
//    std::cout<<"check leader "<< typeid(role).name()<<"  "<<fixed_timer.check_receive()<<"\n";
        if (role == RaftRole::Leader &&
            fixed_timer.check_receive() &&
            rpc_clients_map[my_id]) {
            return {true, current_term};
        }
        /* Lab3: Your code here */
        return std::make_tuple(false, current_term);
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::is_stopped() -> bool
    {
        return stopped.load();
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
    {
        /* Lab3: Your code here */
        if (is_stopped()) {
//            std::cerr<<"client "<<my_id<<" is stop!\n";
            return {false, 0, 0};
        }
        std::lock_guard lockGuard(mtx);
        const auto last_included_idx = get_last_include_idx();
        if (role != RaftRole::Leader) {
            return {false,
                    current_term,
                    last_included_idx + log_storage->size() - 1};
        }
        Command command;
        command.deserialize(cmd_data, cmd_size);
        auto store_idx = log_storage->append_log(current_term, command);
        log_storage->save(current_term, vote_for);

        return {true, current_term, store_idx + last_included_idx};
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
    {
        std::lock_guard lockGuard(mtx);
        const auto idx = commit_idx - get_last_include_idx();
        auto [term, _] = log_storage->get_nth(idx);
        auto data = log_storage->create_snap(commit_idx);
        state->clear();
        state->apply_snapshot(data);
        log_storage->write_snapshot(0, data);
        log_storage->last_snapshot(term, commit_idx);
        log_storage->save(current_term, vote_for);
        return true;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
    {
        std::lock_guard lockGuard(mtx);
        return state->snapshot();
    }

    template <typename StateMachine, typename Command>
    RequestVoteArgs RaftNode<StateMachine, Command>::create_request_vote() {
        auto currentTerm = current_term;
        auto [lastLogTerm, lastLogIdx] = log_storage->get_last();
//    MSGPACK_DEFINE(
//            term,
//            candidate_id,
//            last_log_index,
//            last_log_term
//    )
        RequestVoteArgs requestVoteArgs = {
                currentTerm,
                my_id,
                lastLogIdx,
                lastLogTerm
        };
        return requestVoteArgs;
    };

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::become_follower(const term_id_t term,
                                                          const node_id_t leader) {
        role = RaftRole::Follower;
        timer.start();
        current_term = term;
        vote_for = -1;

        votes_received = 0;
        leader_id = leader;
//    std::cout<<"id "<<my_id<<" become follower of "<<leader<<"\n";

    }
    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::become_candidate() {
        role = RaftRole::Candidate;
        timer.start();
        vote_for = my_id;
//    std::cout<<"id "<<my_id<<" become candidate!\n";
        votes_received = 1;
    }
    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::become_leader() {
//    std::cout<<my_id<<" electing \n";
        if (role == RaftRole::Leader)
            return;
        role = RaftRole::Leader;
        vote_for = -1;
        votes_received = 0;
        leader_id = my_id;
//        std::cout<<" id "<<my_id<<"become leader!\n";
        // initialize the next_idx_map
        auto lastLogIdx = log_storage->size() - 1;

        //set all next idx : initialized to leader last log index + 1
        for (const auto& [idx, cli] : rpc_clients_map) {
            next_index[idx] = lastLogIdx + 1;
        }
        fixed_timer.start();
    }
/******************************************************************

                         Internal RPC Related

*******************************************************************/
    template <typename StateMachine, typename Command>
    bool RaftNode<StateMachine, Command>::is_more_up_to_data(chfs::term_id_t termId,
                                                             int index) {
        auto [lastLogTerm, lastLogIdx] = log_storage->get_last();
        return lastLogTerm > termId || (lastLogTerm == termId && lastLogIdx > index);
    }
    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
    {
        if(is_stopped()) {
//            std::cerr<<"already stop\n";
            return {false,0};
        }
        else {
            /* Lab3: Your code here */
//    1. Reply false if term < currentTerm (§5.1)
            auto node_id = args.candidate_id;
            if (args.term < current_term) {
                return {false, current_term};
            }
            if (vote_for == node_id) {
                if (args.term > current_term) {
                    become_follower(args.term, -1);
                    return {true, current_term};
                }
                vote_for = -1;
                return {false, current_term};
            }
            if (!is_more_up_to_data(args.last_log_term, args.last_log_index)) {
                if (vote_for == my_id && role == RaftRole::Candidate) {
                    votes_received--;
                }
                become_follower(args.term, -1);
                vote_for = node_id;
                return {true, current_term};
            }
            return {false, current_term};
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs &arg, const RequestVoteReply reply)
    {
        /* Lab3: Your code here */
//    std::cout<<my_id<<" handle request from "<<target<<"\n";
        if (reply.term_id > current_term) {
//        std::cout<<my_id<<" not vote\n";
            current_term = reply.term_id;
        }

        if(!reply.is_vote && reply.term_id>current_term){
//            std::cout<<my_id<<" not vote\n";
            become_follower(reply.term_id, -1);
            vote_for = target;
        }
        else if(reply.is_vote && role == RaftRole::Candidate) {
//        std::cout<<my_id<<" trying to become leader\n";
            current_term = std::max(current_term, reply.term_id);
            votes_received++;
            if (votes_received > node_configs.size() / 2) {
                become_leader();
            }
        }

    }
// Entries rpc
    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
    {
        AppendEntriesArgs<Command> args = transform_rpc_append_entries_args<
                Command>(rpc_arg);
//    std::cout<<my_id<<" Append entries from "<<args.leader_id<<"\n";
        // ping info
        if (current_term > args.term) {
            return {false, current_term};
        }
        current_term = args.term;
        if (args.alive) {
            if (current_term <= args.term) {
                vote_for = -1;
                become_follower(args.term, args.leader_id);
            }
            leader_id = args.leader_id;
            timer.receive();
            return {true, current_term};
        }
        auto last_included_idx = get_last_include_idx();
        // fill 0 from last_included_idx to commit_idx
        if (last_included_idx + log_storage->size() - 1 < args.last_included_idx) {
            // missing snapshot before
            // insert 0s
            for (int i = last_included_idx; i < args.last_included_idx; ++i) {
                log_storage->insert(0, Command{0});
            }
        } else if (last_included_idx > args.prev_log_index + args.last_included_idx) {
            // this node has snapshotted a committed value
            // so insert 0s
            auto insert_num = static_cast<int>(args.prev_log_index) - (
                    last_included_idx - args.last_included_idx);
            for (int i = 0; i < insert_num; ++i) {
                log_storage->insert(0, Command(0));
            }
            auto curr_idx = log_storage->size();
            for (auto& entry : args.entries) {
                log_storage->insert_or_rewrite(entry, curr_idx++);
            }
            log_storage->save(current_term, vote_for);
            commit_idx = std::min(last_included_idx + log_storage->size() - 1,
                                  args.leader_commit);
            return {true, current_term};
        }
        if (auto entry = log_storage->get_nth(
                    args.prev_log_index);
                entry.term_id == args.prev_log_term || args.prev_log_index == 0) {
            auto curr_idx = args.prev_log_index + 1;
            for (auto& entry_item : args.entries) {
                log_storage->insert_or_rewrite(entry_item, curr_idx++);
            }
            vote_for=-1;
            log_storage->save(current_term, vote_for);
            commit_idx = std::min(last_included_idx + log_storage->size() - 1,
                                  args.leader_commit);
            return {true, current_term};
        }
        log_storage->delete_after_nth(args.prev_log_index);
        commit_idx = std::min(last_included_idx + log_storage->size() - 1,
                              args.leader_commit);
        return {false, current_term};
    }
    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::recover_from_disk() {
        // recover logs
        auto res = log_storage->recover();
        std::tie(current_term, vote_for) = res;
//    std::cout<<"++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ recover : "<<current_term <<" "<<vote_for<<"  \n";
        // recover data
        auto data = log_storage->get_snapshot_data();

        if (data.first == 0) return;
        std::stringstream ss;
        ss << data.first;
        for (int i = 0; i < data.first; ++i) {
            ss << ' ' << data.second[i];
        }
        std::string str = ss.str();
        std::vector<u8> buffer(str.begin(), str.end());
        state->apply_snapshot(buffer);
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_append_entries_reply(
            int node_id,
            const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
    {

        if(!reply.accepted){
            if(reply.term > current_term){
                become_follower(reply.term,node_id);
                return;
            }
//        std::cout<<"Accepting =========================: "<<my_id<<" "<<arg.prev_log_index<<std::endl;
            next_index[node_id]=arg.prev_log_index;
            return;
        }
        // if accepted
        if(arg.alive){
            fixed_timer.receive();
            return;
        }
        auto last_included_idx = get_last_include_idx();
        auto received_idx = arg.prev_log_index + arg.entries.size();
        next_index[node_id] = received_idx + 1;
        const auto cnt = node_configs.size();
        for(auto idx = last_included_idx + arg.prev_log_index+1;
            idx <= last_included_idx + received_idx ; ++idx){
            if (!applied_map[idx]) {
                applied_map[idx] = 1;
            }
            applied_map[idx] ++;
            //最新commit或超过半数apply
            if(idx <= commit_idx ||
               applied_map[idx] > cnt / 2){
                if(role!=RaftRole::Leader)
                    continue;
                commit_idx = std::max(idx,commit_idx);
            }
        }
    }



    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
    {
        /* Lab3: Your code here */
        if (current_term > args.term) {
            return {current_term};
        }
        if (args.last_included_idx == get_last_include_idx()) {
            return {current_term};
        }
        if (args.offset == 0) {
            installing_data.clear();
        }
        // append args.data to installing_data
        installing_data.insert(installing_data.end(), args.data.begin(),
                               args.data.end());
        log_storage->write_snapshot(
                args.offset,
                args.data
        );
        if (args.done) {
            log_storage->last_snapshot(args.last_included_term,
                                       args.last_included_idx);
            log_storage->save(current_term, vote_for);
            state->clear();
            this->state->apply_snapshot(installing_data);
            installing_data.clear();
        }
        std::string str(args.data.begin(), args.data.end());
        std::stringstream ss(str);
        int size, tmp;
        ss >> size;
        for (int i = 0; i < size; ++i) {
            ss >> tmp;
        }
        return {current_term};
    }


    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
    {
        /* Lab3: Your code here */
        if (reply.term > current_term) {
            current_term = reply.term;
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
    {
        if (rpc_clients_map[target_id] == nullptr
            || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
            return;
        }
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
        clients_lock.unlock();
        if (res.is_ok()) {
//            std::cout<<my_id<<" sending vote "<<target_id<<"\n";
            handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
        } else {
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
    {

        std::unique_lock<std::mutex> clients_lock(clients_mtx);
//    std::cout<<my_id<< " begin to send append entries to "<<target_id<<"\n";
//    std::cout<<target_id<<"   "<<(rpc_clients_map[target_id]!= nullptr)<<std::endl;
        if (rpc_clients_map[target_id] == nullptr||
            rpc_clients_map[target_id]->get_connection_state()!= rpc::client::connection_state::connected) {
//            std::cerr<<target_id<<" lose connection\n";
            return;
        }
        RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
//        std::cout<<my_id<< " send append entries to "<<target_id<<"\n";
        auto res =rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
        clients_lock.unlock();
        if (res.is_ok()) {

            handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
        }
        else {
//            std::cerr<<"failed\n";
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr
            || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
            return;
        }
        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
        clients_lock.unlock();
        if (res.is_ok()) {
            handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
        } else {
//            std::cerr<<target_id<<" fails\n";
        }
    }


/******************************************************************

                        Background Workers

*******************************************************************/
//    Complete the method RaftNode::run_background_election,
//    which should turn to candidate and
//    start an election after a leader timeout by sending request_vote RPCs asynchronously.
    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_election() {
        // Periodly check the liveness of the leader.

        // Work for followers and candidates.
        while (true) {
            {
                if (is_stopped()) {
                    return;
                }
                std::this_thread::sleep_for(timer.sleep_for());
                if (role == RaftRole::Leader) {
                    continue;
                }
                if(!rpc_clients_map[my_id]) {
                    role = RaftRole::Follower;
                    continue;
                }

                const auto receive = timer.check_receive();
                const auto timeout = timer.timeout();
                if (role == RaftRole::Follower) {
                    if (timeout && !receive) {
                        // trying to become candidate
                        become_candidate();
                    }
                }
                if(!timeout||receive){
                    continue;
                }
                current_term ++;
                vote_for = my_id;
                votes_received = 1;
                auto args = create_request_vote();
                for(const auto &[index,cli]:rpc_clients_map){
                    if(index == my_id) continue;
                    thread_pool->
                            enqueue(&RaftNode::send_request_vote, this,index,args);
                }
            }
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_commit() {
        // Periodly send logs to the follower.

        // Only work for the leader.

        /* Uncomment following code when you finish */
        while (true) {
            if (is_stopped()) {
                return;
            }
            std::this_thread::sleep_for(fixed_timer.sleep_for());
            if (role != RaftRole::Leader) continue;
            if (!fixed_timer.check_receive()) {
                continue;
                /* Lab3: Your code here */
            }
            //reset the time
            fixed_timer.reset();
            for(const auto &[node_id, client] : rpc_clients_map){
                if(node_id == my_id)continue;
//                std::cout<<my_id<<" is sending commits to "<<node_id<<"\n";
                const auto next_idx = next_index[node_id];
//                 std::cout<<node_id<<"==============================================a"<<"\n";
                auto prev_idx = next_idx - 1;
//                 std::cout<<node_id<<"==============================================b"<<"\n";
//                 std::cout<<"size : "<<log_storage->size()<<" prev idx: "<<prev_idx<<std::endl;
                if(log_storage->size() - 1 < prev_idx) {
                    continue;
                }
//                 std::cout<<node_id<<"==============================================c"<<"\n";
                auto [prev_term, _] = log_storage->get_nth(prev_idx);
                auto entries = log_storage->get_after_nth(prev_idx);
//                 term_id_t term;
//                 node_id_t leader_id;
//                 commit_id_t prev_log_index;
//                 term_id_t prev_log_term;
//                 commit_id_t leader_commit;
//                 std::vector<LogEntry<Command>> entries;
//                 bool alive ;
//                 int last_included_idx;
//                std::cout<<"====> "<<current_term<<" "<<prev_idx<<" "<<prev_term<<" "<<commit_idx<<" "<<entries.size()<<" "<<commit_idx<<std::endl;
                auto arg = AppendEntriesArgs<Command>{
                        current_term,
                        my_id,
                        prev_idx,
                        prev_term,
                        commit_idx,
                        entries,
                        false,
                        get_last_include_idx()
                };
//                 std::cout<<"begin to send ------------>\n";
                thread_pool->enqueue(
                        &RaftNode::send_append_entries,
                        this,
                        node_id,
                        arg);
            }
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_apply() {
        // Periodly apply committed logs the state machine

        while (true) {
            {
                if (is_stopped()) {
                    return;
                }
                std::this_thread::sleep_for(fixed_timer.sleep_for());
                auto last_idx = get_last_include_idx();

                auto last_apply = state->get_applied_size();
                const int this_applied = commit_idx;
                for (int idx = last_apply - last_idx;
                     idx <= this_applied - last_idx; ++idx) {
                    if (idx < 0) continue;
                    auto term = log_storage->get_nth(idx).term_id;
                    if (term == 0) continue;
                    auto cmd = log_storage->get_nth(idx).command;
                    state->apply_log(cmd);
                }
            }
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_ping() {
        while (true) {
            {
                if (is_stopped()) {
//                    std::cout<<"stop!!!!\n";
                    return;
                }
                std::this_thread::sleep_for(fixed_timer.sleep_for());
                if (role != RaftRole::Leader) {
                    continue;
                }
//            MSGPACK_DEFINE(
//                    term,
//                    leader_id,
//                    prev_log_index,
//                    prev_log_term,
//                    leader_commit,
//                    entries,
//                    alive,
//                    last_included_idx
//            )
//                std::cout<<my_id<<" ping !\n";
                AppendEntriesArgs<Command> args{
                        current_term,
                        my_id,
                        0,
                        0,
                        commit_idx,
                        std::vector<LogEntry<Command>>(),
                        true,
                        get_last_include_idx()
                };
                for (const auto& [idx, cli] : rpc_clients_map) {
                    if (idx == my_id) continue;
                    if (!cli) continue;
                    thread_pool->enqueue(
                            &RaftNode::send_append_entries,
                            this,
                            idx,
                            args);
                }
            };
        }
    }
    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_snapshot() {
        while (true) {
            if (is_stopped()) {
                return;
            }
            std::this_thread::sleep_for(fixed_timer.sleep_for());
            if (role != RaftRole::Leader) {
                continue;
            }
            auto data = log_storage->get_snapshot_data();
            if (data.first == 0) continue;
            std::stringstream ss;
            ss << data.first;
            for (int i = 0; i < data.first; ++i) {
                ss << ' ' << data.second[i];
            }
            std::string str = ss.str();
            std::vector<u8> buffer(str.begin(), str.end());
            InstallSnapshotArgs args{
                    current_term, my_id, log_storage->get_last_include_idx(),
                    log_storage->get_last_include_term(), 0, buffer, true};
            for (const auto& [cli_idx, cli] : rpc_clients_map) {
                next_index[cli_idx] = log_storage->size();
                if (cli_idx != my_id) {
                    thread_pool->enqueue(&RaftNode::send_install_snapshot, this, cli_idx,
                                         args);
                }
            }
        }
    }
/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);

        /* turn off network */
        if (!network_availability[my_id]) {
            for (auto &&client: rpc_clients_map) {
                if (client.second != nullptr)
                    client.second.reset();
            }

            return;
        }

        for (auto node_network: network_availability) {
            int node_id = node_network.first;
            bool node_status = node_network.second;

            if (node_status && rpc_clients_map[node_id] == nullptr) {
                RaftNodeConfig target_config;
                for (auto config: node_configs) {
                    if (config.node_id == node_id)
                        target_config = config;
                }

                rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
            }

            if (!node_status && rpc_clients_map[node_id] != nullptr) {
                rpc_clients_map[node_id].reset();
            }
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::set_reliable(bool flag)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        for (auto &&client: rpc_clients_map) {
            if (client.second) {
                client.second->set_reliable(flag);
            }
        }
    }

    template <typename StateMachine, typename Command>
    int RaftNode<StateMachine, Command>::get_list_state_log_num()
    {
        /* only applied to ListStateMachine*/
        std::unique_lock<std::mutex> lock(mtx);

        return state->num_append_logs;
    }

    template <typename StateMachine, typename Command>
    int RaftNode<StateMachine, Command>::rpc_count()
    {
        int sum = 0;
        std::unique_lock<std::mutex> clients_lock(clients_mtx);

        for (auto &&client: rpc_clients_map) {
            if (client.second) {
                sum += client.second->count();
            }
        }

        return sum;
    }

    template <typename StateMachine, typename Command>
    std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
    {
        if (is_stopped()) {
            return std::vector<u8>();
        }

        std::unique_lock<std::mutex> lock(mtx);
        return state->snapshot();
    }

}