#include "raft_server.h"

#define FMT_HEADER_ONLY
#include "hlib/spdlog/spdlog.h"

namespace spd = spdlog;

namespace zraft {

static int random(int l, int r) {
    static default_random_engine e;
    e.seed(time(nullptr));
    l *= 10, r *= 10;
    uniform_int_distribution<int> d(l, r);
    return d(e);
}

RaftServer::RaftServer(int port, const vector<int> &peers)
    : id_(port),
      port_(port),
      status_(Status::follower),
      voted_(false),
      stop_(false),
      pool_(20) {
    for (int peer : peers) {
        if (peer == id_) continue;
        peers_.emplace(peer, "http://localhost:" + to_string(peer));
        spd::info("{} | peer {}", id_, peer);
    }

    initRpc();

    initDaemons();

    spd::info("{} | listen...", id_);
    srv_.listen("0.0.0.0", port_);
}

RaftServer::~RaftServer() {
    stop_ = true;
    for (auto &[name, th] : daemons_) {
        th.join();
    }
}

void RaftServer::initRpc() {
    srv_.Post("/RequestVote",
              [this](const htp::Request &req, htp::Response &res) {
                  this->requestVote(req, res);
              });
    srv_.Post("/AppendEntries",
              [this](const htp::Request &req, htp::Response &res) {
                  this->appendEntries(req, res);
              });
}

void RaftServer::requestVote(const htp::Request &req, htp::Response &res) {
    nh::json req_json = nh::json::parse(req.body);
    spd::info("{} | requestVote | req: {}", id_, req_json.dump());
    int id = req_json["id"].get<int>();
    int term = req_json["term"].get<int>();
    int log_index = req_json["log_index"].get<int>();

    nh::json res_json;
    res_json["id"] = id_;
    res_json["vote"] = false;

    if (!voted_ && (term_ < term || term_ == term && log_index_ <= log_index)) {
        res_json["vote"] = true;
        voted_ = true;
        status_ = Status::follower;
    }

    string res_body = res_json.dump();
    spd::info("{} | requestVote | res: {}\n", id_, res_body);
    res.set_content(res_body, "application/json");
}

void RaftServer::appendEntries(const htp::Request &req, htp::Response &res) {
    nh::json req_json = nh::json::parse(req.body);
    spd::info("{} | appendEntries | req: {}", id_, req_json.dump());

    heartbeat_ = true;

    if (req_json.contains("leader_id")) {
        leader_id_ = req_json["leader_id"].get<int>();
        status_ = Status::follower;
    }

    spd::info("{} | leader is server {}", id_, leader_id_);
    int id = req_json["id"].get<int>();
    int term = req_json["term"].get<int>();
    int log_index = req_json["log_index"].get<int>();

    nh::json res_json;
    res_json["id"] = id_;
    res_json["ok"] = "ok";
    string res_body = res_json.dump();
    spd::info("{} | appendEntries | res: {}", id_, res_body);
    res.set_content(res_body, "application/json");
}

void RaftServer::initDaemons() {
    daemons_.emplace("timeoutElect", [this] { this->timeoutElect(); });
}

void RaftServer::timeoutElect() {
    while (true) {
        heartbeat_ = false;
        voted_ = false;
        // 使用睡眠模拟定时器
        auto election_timeout = chrono::milliseconds(random(100, 200));
        this_thread::sleep_for(election_timeout);

        if (stop_) break;
        if (status_ != Status::follower) continue;
        if (heartbeat_) continue;

        // 心跳超时，变为候选人，发送选举请求
        status_ = Status::candidate;
        spd::info("{} | start elect...", id_);
        int vote_count = 1;  // 先投自己一票,但不改变voted的状态
        nh::json req_json;
        req_json["id"] = id_;
        req_json["term"] = term_;
        req_json["log_index"] = log_index_;
        string req_body = req_json.dump();

        // 防止阻塞，使用线程池发送消息
        unordered_map<int, future<htp::Result>> futures;
        for (auto &[id, cli] : peers_) {
            futures.emplace(id, pool_.put([&] {
                return cli.Post("/RequestVote", req_body, "application/json");
            }));
        }

        // 计票
        for (auto &[id, f] : futures) {
            auto res = f.get();
            if (!res || res->status != 200) {
                spd::warn(
                    "{} | timeoutElect | server {} post failed! status: {}",
                    id_, id, res->status);
                continue;
            }
            spd::info("{} | timeoutElect | server {} res: {}", id_, id,
                      res->body);
            nh::json res_json = nh::json::parse(res->body);
            bool is_voted = res_json["vote"].get<bool>();
            vote_count += is_voted;
        }
        if (status_ == Status::follower) continue;
        // 投票超过半数则成为 leader，否则成为 follower 重新开始
        int f = (peers_.size() + 1) / 2;
        if (vote_count <= f) {
            status_ = Status::follower;
            continue;
        }
        status_ = Status::leader;

        // todo leader应该执行的行为
        daemons_.emplace("lead", [this] { this->lead(); });
    }
}

void RaftServer::lead() {
    spd::info("{} | timeoutElect | I am the leader!", id_);
    leader_id_ = id_;
    voted_ = false;
    {
        // 防止阻塞，使用线程池发送消息
        nh::json req_json;
        req_json["leader_id"] = id_;
        req_json["id"] = id_;
        req_json["term"] = term_;
        req_json["log_index_"] = log_index_;
        string req_body = req_json.dump();
        unordered_map<int, future<htp::Result>> futures;
        for (auto &[id, cli] : peers_) {
            auto res = pool_.put([&] {
                return cli.Post("/AppendEntries", req_body, "application/json");
            });
            futures.emplace(id, move(res));
        }
        for (auto &[id, f] : futures) {
            auto res = f.get();
            if (!res || res->status != 200) {
                spd::warn("{} | lead | server {} post failed!", id_, id);
                continue;
            }
            nh::json res_json = nh::json::parse(res->body);
            spd::info("{} | lead | server {} res: {}", id_, id,
                      res_json.dump());
        }
    }
    while (status_ == Status::leader) {
        int broacast_time = random(40, 60);
        this_thread::sleep_for(chrono::milliseconds(broacast_time));
        // 防止阻塞，使用线程池发送消息
        nh::json req_json;
        req_json["id"] = id_;
        req_json["term"] = term_;
        req_json["log_index"] = log_index_++;
        string req_body = req_json.dump();
        unordered_map<int, future<htp::Result>> futures;
        for (auto &[id, cli] : peers_) {
            auto res = pool_.put([&] {
                return cli.Post("/AppendEntries", req_body, "application/json");
            });
            futures.emplace(id, move(res));
        }
        for (auto &[id, f] : futures) {
            auto res = f.get();
            if (!res || res->status != 200) {
                spd::warn("{} | lead | server {} post failed!", id_, id);
                continue;
            }
            nh::json res_json = nh::json::parse(res->body);
            spd::info("{} | lead | server {} res: {}", id_, id,
                      res_json.dump());
        }
    }
}

}  // namespace zraft