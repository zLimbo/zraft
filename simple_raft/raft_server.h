#ifndef RAFT_SERVER_H
#define RAFT_SERVER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "hlib/httplib.h"
#include "hlib/json.hpp"
#include "hlib/thread_pool.h"

using namespace std;
namespace htp = httplib;
namespace nh = nlohmann;

namespace zraft {

class RaftServer {
public:
    enum class Status { follower, candidate, leader };

    inline static unordered_map<Status, string_view> kStatusMap = {
        {Status::follower, "follower"},
        {Status::candidate, "candidate"},
        {Status::leader, "leader"}};

public:
    RaftServer(int port, const vector<int> &peers);

    ~RaftServer();

    void initRpc();

    void requestVote(const htp::Request &req, htp::Response &res);

    void appendEntries(const htp::Request &req, htp::Response &res);

    void initDaemons();

    void timeoutElect();

    void lead();

private:
    int term_ = 0;

    int log_index_ = 0;

    vector<pair<int, string>> logs_;

    atomic<int> leader_id_;

    int id_;

    // condition_variable cond_is_follower_;

    unordered_map<int, htp::Client> peers_;

    atomic<Status> status_;

    atomic<bool> heartbeat_;

    atomic<bool> voted_;

    atomic<bool> stop_;

    unordered_map<string_view, thread> daemons_;

    htp::Server srv_;

    int port_;

    ThreadPool pool_;
};

}  // namespace zraft

#endif  // RAFT_SERVER_H