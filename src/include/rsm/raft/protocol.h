#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
    /* Lab3: Your code here */
    term_id_t term;
    node_id_t candidate_id;
    commit_id_t last_log_index;
    term_id_t last_log_term;
    MSGPACK_DEFINE(
    term,
    candidate_id,
    last_log_index,
    last_log_term
    )
};

struct RequestVoteReply {
    /* Lab3: Your code here */
    bool is_vote;
    term_id_t term_id;
    MSGPACK_DEFINE(
    is_vote,
    term_id
    )
};

template <typename Command>
struct AppendEntriesArgs {
    /* Lab3: Your code here */
    term_id_t term;
    node_id_t leader_id;
    commit_id_t prev_log_index;
    term_id_t prev_log_term;
    commit_id_t leader_commit;
    std::vector<LogEntry<Command>> entries;
    bool alive ;
    int last_included_idx;
    MSGPACK_DEFINE(
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            entries,
            alive,
            last_included_idx
    )
};

struct RpcAppendEntriesArgs {
    /* Lab3: Your code here */
    std::vector<u8> data;
    MSGPACK_DEFINE(
    data
    )
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    /* Lab3: Your code here */
    std::stringstream ss;
    std::vector<LogEntry<Command>> entries;
    ss  << arg.term<< ' '
        << arg.leader_id<< ' '
        << arg.prev_log_index << ' '
        << arg.prev_log_term<< ' '
        << arg.leader_commit<<' '
        << arg.alive<<' '
        << arg.last_included_idx<<' '
        << static_cast<int>(arg.entries.size());
    for (const auto& entry : arg.entries) {
        ss << ' ' << entry.term_id << ' ' << entry.command.value;
    }
    std::string str = ss.str();
    std::cout<<"[ARG SEND] \t\t"<<str<<std::endl;
    return RpcAppendEntriesArgs{{str.begin(), str.end()}};
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    /* Lab3: Your code here */
    AppendEntriesArgs<Command> arg;
    int size ;

    std::stringstream ss({rpc_arg.data.begin(),rpc_arg.data.end()});

    ss  >> arg.term
        >> arg.leader_id
        >> arg.prev_log_index
        >> arg.prev_log_term
        >> arg.leader_commit
        >> arg.alive
        >> arg.last_included_idx
        >> size;
    std::cout<<"[ARG RECEIVE] \t"
                           << arg.term<< ' '
                           << arg.leader_id<< ' '
                           << arg.prev_log_index << ' '
                           << arg.prev_log_term<< ' '
                           << arg.leader_commit<<' '
            <<  arg.alive<<' '
            << arg.last_included_idx<<' '
                           << size <<' ';
    term_id_t term = 0;
    int cmd;
    for (int i = 0; i < size; ++i) {
        ss >> term >> cmd;
        arg.entries.emplace_back(LogEntry<Command>{term, cmd});
    }
    std::cout<<std::endl;
    return arg;
}

struct AppendEntriesReply {
    /* Lab3: Your code here */
    bool accepted;
    term_id_t term;
    MSGPACK_DEFINE(
    accepted,
    term
    )
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */
    term_id_t term;
    node_id_t leader_id;
    int last_included_idx;
    term_id_t last_included_term;
    int offset;
    std::vector<u8> data;
    bool done;

    MSGPACK_DEFINE(
        term,
        leader_id,
        last_included_idx,
        last_included_term,
        offset,
        data,
        done
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */
    term_id_t term;
    MSGPACK_DEFINE(
        term
    )
};

} /* namespace chfs */