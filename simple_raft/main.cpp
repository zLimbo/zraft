
#include "raft_server.h"

int main() {
    int num = 3;
    vector<int> peers;
    for (int i = 0; i < num; ++i) {
        peers.push_back(8001 + i);
    }

    vector<thread> ths;
    for (int i = 0; i < num; ++i) {
        ths.emplace_back([i, &peers] { zraft::RaftServer(peers[i], peers); });
    }

    for (auto &th : ths) {
        th.join();
    }

    printf("done");
    return 0;
}

// int main(int argc, char **argv) {
//     int num = 2;
//     vector<int> peers;
//     for (int i = 0; i < num; ++i) {
//         peers.push_back(8001 + i);
//     }

//     int idx = stoi(argv[1]);
//     zraft::RaftServer srv(peers[idx], peers);

//     printf("done");
//     return 0;
// }