#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "hlib/httplib.h"
#include "hlib/json.hpp"

using namespace std;
namespace htb = httplib;
namespace nhm = nlohmann;

class RaftServer {
public:
    RaftServer(int port) : port_(port), id_(port) {
        srv_.Post("/RequestVote",
                  [this](const htb::Request &req, htb::Response &res) {
                      requestVote(req, res);
                  });

        srv_.listen("0.0.0.0", port_);
    }

    void requestVote(const htb::Request &req, htb::Response &res) {
        nhm::json req_json = nhm::json::parse(req.body);
        printf("req: %s\n", req_json.dump().c_str());
        int id = req_json["id"].get<int>();
        int term = req_json["term"].get<int>();
        int log_index = req_json["log_index"].get<int>();

        nhm::json res_json;
        res_json["id"] = id_;
        res_json["vote"] = true;

        if (term < term_ || term == term_ && log_index < log_index_) {
            res_json["vote"] = false;
        }
        string res_body = res_json.dump();
        printf("res: %s\n", res_body.c_str());
        res.set_content(res_body, "application/json");
    }

private:
    int term_ = 0;
    int log_index_ = 0;
    map<int, pair<int, string>> logs_;

    int leader_id_;
    int id_;

    htb::Server srv_;
    int port_;
};

int main() {
    RaftServer srv(8001);
    return 0;
}