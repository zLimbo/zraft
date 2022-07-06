package raft

import (
	"bufio"
	"fmt"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"zpbft/zpbft/config"
	"zpbft/zpbft/master"
	"zpbft/zpbft/util"
	"zpbft/zpbft/zlog"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type State = int32

const (
	Follower State = iota
	Candidate
	Leader
)

var state2str = map[State]string{
	Follower:  "follower",
	Candidate: "candidate",
	Leader:    "leader",
}

const (
	intervalTime         = 20
	heartbeatTime        = 200
	electionTimeoutFrom  = 600
	electionTimeoutRange = 200
	leaderTimeout        = 2000
)

func GetRandomElapsedTime() int {
	return electionTimeoutFrom + rand.Intn(electionTimeoutRange)
}

type Peer struct {
	id         int
	addr       string
	rpcCli     *rpc.Client // rpc服务端的客户端
	connStatus int32
}

type Raft struct {
	mu         sync.Mutex // Lock to protect shared access to this peer's state
	peers      []*Peer    // RPC end points of all peers
	me         int        // this peer's index into peers[]
	addr       string     // rpc监听地址
	state      State
	applyCh    chan ApplyMsg
	leaderLost int32 // leader 是否超时

	// raft参数
	leaderId    int
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

func RunRaft(maddr, saddr string) {

	rf := &Raft{
		addr:        saddr,
		state:       Follower,
		leaderId:    -1,
		applyCh:     make(chan ApplyMsg),
		currentTerm: 0,
		votedFor:    -1,
		log:         []LogEntry{{Term: 0}}, // 初始存一个默认entry，索引和任期为0
		commitIndex: 0,
		lastApplied: 0,
	}

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| make raft, peers.num:%d",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		len(rf.peers))

	// 开启rpc服务
	rf.rpcListen()

	// 注册节点信息，并获取其他节点信息
	rf.register(maddr)

	// 并行建立连接
	rf.connectPeers()

	time.Sleep(time.Second)

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyLog()

	go rf.test()

	select {}
}

func (rf *Raft) rpcListen() {
	// 放入协程防止阻塞后面函数
	go func() {
		rpc.Register(rf)
		rpc.HandleHTTP()
		err := http.ListenAndServe(rf.addr, nil)
		if err != nil {
			zlog.Error("http.ListenAndServe failed, err:%v", err)
		}
	}()
}

func (rf *Raft) register(maddr string) {
	time.Sleep(500 * time.Millisecond)

	// 连接master,注册节点信息，并获取其他节点信息
	zlog.Info("connect master ...")
	rpcCli, err := rpc.DialHTTP("tcp", maddr)
	if err != nil {
		zlog.Error("rpc.DialHTTP failed, err:%v", err)
	}

	args := &master.RegisterArgs{Addr: rf.addr}
	reply := &master.RegisterReply{}

	zlog.Info("register peer info ...")
	rpcCli.Call("Master.Register", args, reply)

	// 重复addr注册或超过 PeerNum 限定节点数，注册失败
	if !reply.Ok {
		zlog.Error("register failed")
	}

	// 设置节点信息
	rf.peers = make([]*Peer, len(reply.Addrs))
	for i, addr := range reply.Addrs {
		if addr == rf.addr {
			rf.me = i
		}
		rf.peers[i] = &Peer{
			id:   i,
			addr: addr,
		}
	}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	zlog.Info("register success")
}

func (rf *Raft) connectPeers() {
	zlog.Info("build connect with other peers ...")
	// 等待多个连接任务结束再继续
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)
	for id, peer := range rf.peers {
		if id == rf.me {
			continue
		}
		p := peer
		go func() {
			// 每隔1s请求建立连接，60s未连接则报错
			t0 := time.Now()
			for time.Since(t0).Seconds() < 60 {
				rpcCli, err := rpc.DialHTTP("tcp", p.addr)
				if err == nil {
					zlog.Debug("connect (id=%d,addr=%s) success", p.id, p.addr)
					p.rpcCli = rpcCli
					wg.Done()
					return
				}
				zlog.Warn("connect (id=%d,addr=%s) error, err:%v", p.id, p.addr, err)
				time.Sleep(time.Second)
			}
			zlog.Error("connect (id=%d,addr=%s) failed, terminate", p.id, p.addr)
		}()
	}
	wg.Wait()
	zlog.Info("==== connect all peers success ====")
}

func (rf *Raft) ticker() {
	for {
		// 如果已经是leader，则不运行超时机制，睡眠一个心跳时间
		if atomic.LoadInt32(&rf.state) != Leader {
			time.Sleep(heartbeatTime * time.Millisecond)
		}

		// 先将失效设置为1，leader的心跳包会将该值在检测期间重新置为 0
		atomic.StoreInt32(&rf.leaderLost, 1)

		// 失效时间在[electionTimeout, 2 * electionTimeout)间随机
		elapsedTime := GetRandomElapsedTime()
		time.Sleep(time.Duration(elapsedTime) * time.Millisecond)

		// 如果超时且不是Leader且参加选举
		if atomic.LoadInt32(&rf.leaderLost) == 1 && atomic.LoadInt32(&rf.state) != Leader {
			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| elapsedTime=%d (ms)",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term, elapsedTime)
			rf.elect()
		}
	}
}

// 选举
func (rf *Raft) elect() {
	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| state:%s=>candidate, elect",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		state2str[atomic.LoadInt32(&rf.state)])

	args := func() *RequestVoteArgs {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.currentTerm += 1                     // 自增自己的当前term
		atomic.StoreInt32(&rf.state, Candidate) // 身份先变为candidate
		rf.leaderId = -1                        // 无主状态
		rf.votedFor = rf.me                     // 竞选获得票数，自己会先给自己投一票，若其他人请求投票会失败

		return &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
	}()
	// 选举票号统计，1为自己给自己投的票
	ballot := 1
	// 向所有peer发送请求投票rpc
	for server := range rf.peers {
		// 排除自身
		if server == rf.me {
			continue
		}
		// 并发请求投票，成为leader的逻辑也在此处理
		server1 := server
		go func() {
			// TODO 是否需要判断下已经成为leader而不用发送请求投票信息？
			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| request vote %d, start",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				server1)

			reply := &RequestVoteReply{}
			before := time.Now()
			ok := rf.sendRequestVote(server1, args, reply)
			take := time.Since(before)
			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| request vote %d, ok=%v, take=%v",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				server1, ok, take)
			if !ok {
				// TODO 投票请求重试
				return
			}
			// 计票
			rf.ballotCount(server1, &ballot, args, reply)
		}()
	}
}

func (rf *Raft) ballotCount(server int, ballot *int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 超时后过期的消息
	if args.Term != rf.currentTerm {
		zlog.Trace("%d|%2d|%d|%d|<%d,%d>| request vote %d, ok but expire, args.Term=%d, rf.currentTerm=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			server, args.Term, rf.currentTerm)
		return
	}

	if !reply.VoteGranted {
		// 如果竞选的任期落后，则更新本节点的term，终止竞选
		if rf.currentTerm < reply.Term {
			zlog.Info("%d|%2d|%d|%d|<%d,%d>| state:%s=>follower, higher term from %d",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				state2str[atomic.LoadInt32(&rf.state)], server)
			atomic.StoreInt32(&rf.state, Follower)
			rf.currentTerm = reply.Term
			rf.leaderId = -1
			rf.votedFor = -1
		}
		return
	}
	// 增加票数
	*ballot++
	zlog.Trace("%d|%2d|%d|%d|<%d,%d>| request vote %d, peer.num=%d, current ballot=%d",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		server, len(rf.peers), *ballot)
	// 须获得半数以上的投票才能成为leader
	if *ballot*2 <= len(rf.peers) {
		return
	}
	// 如果状态已不是candidate，则无法变为leader
	if atomic.LoadInt32(&rf.state) == Candidate {
		// 本节点成为 leader
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| state:%s=>leader",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			state2str[atomic.LoadInt32(&rf.state)])
		atomic.StoreInt32(&rf.state, Leader)
		rf.leaderId = rf.me
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		go rf.timingHeartbeatForAll()
		go rf.appendEntriesForAll()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ms := config.KConf.DelayFrom + rand.Intn(config.KConf.DelaRange)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	if err := rf.peers[server].rpcCli.Call("Raft.RequestVote", args, reply); err != nil {
		zlog.Warn("%v", err)
		return false
	}
	ms = config.KConf.DelayFrom + rand.Intn(config.KConf.DelaRange)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	return true
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {

	// TODO: 先不管效率，加锁保护VoteFor变量
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	// 新的任期重置投票权
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		atomic.StoreInt32(&rf.state, Follower) // 如果是leader或candidate重新变回followe
		// TODO 请求投票如果有更大任期则不能重置超时！！！
		// atomic.StoreInt32(&rf.leaderLost, 0)   // 重置超时flag
	}

	if rf.currentTerm == args.Term &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(lastLogTerm < args.LastLogTerm ||
			lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| vote for %d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| reject vote for %d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.CandidateId)
	}

	return nil
}

// 发送心跳
func (rf *Raft) timingHeartbeatForAll() {

	zlog.Trace("%d|%2d|%d|%d|<%d,%d>| timingHeartbeatForAll",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term)

	half := len(rf.peers) / 2
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		atomic.StoreInt32(&rf.peers[server].connStatus, 0)
	}

	for atomic.LoadInt32(&rf.state) == Leader {
		peerConnBmap := make(map[int]struct{})
		for t := 0; t < leaderTimeout; t += heartbeatTime {
			for server := range rf.peers {
				if server == rf.me {
					continue
				}
				// 如果heartbeatTime时间内未发送rpc，则发送心跳
				if atomic.LoadInt32(&rf.peers[server].connStatus) == 0 {
					go rf.heartbeatForOne(server)
				} else {
					atomic.StoreInt32(&rf.peers[server].connStatus, 0)
					peerConnBmap[server] = struct{}{}
				}
			}
			// 定时发送心跳
			time.Sleep(heartbeatTime * time.Millisecond)
		}

		// 每 leaderTimeout 时间统计连接状态，如果半数未连接则退出leader状态
		connectCount := len(peerConnBmap)
		if connectCount >= half {
			continue
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 如果连接数少于半数，则退化为 follower
		zlog.Info("%d|%2d|%d|%d|<%d,%d>| leader timeout, connect count=%d(<%d), state:%s=>follower",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			connectCount, half, state2str[atomic.LoadInt32(&rf.state)])
		atomic.StoreInt32(&rf.state, Follower)
		rf.leaderId = -1
		return
	}
}

func (rf *Raft) heartbeatForOne(server int) {
	args, stop := func() (*AppendEntriesArgs, bool) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if atomic.LoadInt32(&rf.state) != Leader {
			return nil, true
		}
		return &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
		}, false
	}()

	if stop {
		return
	}

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| heartbeat to %d, start",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		server)

	reply := &AppendEntriesReply{}
	before := time.Now()
	ok := rf.sendAppendEntries(server, args, reply)
	take := time.Since(before)

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| heartbeat to %d, ok=%v, take=%v",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		server, ok, take)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if atomic.LoadInt32(&rf.state) != Leader {
		return
	}

	// 超时后过期的回复消息
	if args.Term < rf.currentTerm {
		zlog.Trace("%d|%2d|%d|%d|<%d,%d>| heartbeat to %d, ok but expire, args.Term=%d, rf.currentTerm=%d, take=%v",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			server, args.Term, rf.currentTerm, take)
		return
	}

	// 保持连接置位
	atomic.StoreInt32(&rf.peers[server].connStatus, 1)

	if rf.currentTerm < reply.Term {
		zlog.Info("%d|%2d|%d|%d|<%d,%d>| heartbeat to %d, higher term, state:%s=>follower",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			server, state2str[atomic.LoadInt32(&rf.state)])
		rf.currentTerm = reply.Term
		atomic.StoreInt32(&rf.state, Follower)
		rf.leaderId = -1
		return
	}
}

func (rf *Raft) appendEntriesForAll() {
	zlog.Trace("%d|%2d|%d|%d|<%d,%d>| appendEntriesForAll",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.timingAppendEntriesForOne(server)
	}
}

func (rf *Raft) timingAppendEntriesForOne(server int) {
	zlog.Trace("%d|%2d|%d|%d|<%d,%d>| timingAppendEntriesForOne, server=%d",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		server)
	logMatched := false
	nEntriesCopy := 0
	commitIndex := 0

	// TODO: 串行发送rpc，并行会遇到state不是leader却依然发送和处理信息的情况, 也存在rpc幂等性问题
	for atomic.LoadInt32(&rf.state) == Leader {

		// 前一个rpc发送成功，立即判断是否要发下一个
		for atomic.LoadInt32(&rf.state) == Leader {
			// 检查是否已不是leader或被kill
			needSend := func() bool {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 有新的日志需要同步或有更新的commitIndex
				return len(rf.log)-1 >= rf.nextIndex[server] || commitIndex != rf.commitIndex
			}()
			if needSend {
				break
			}
			time.Sleep(intervalTime * time.Millisecond)
		}

		// go rf.heartbeatForOne(server)

		args, stop := func() (*AppendEntriesArgs, bool) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if atomic.LoadInt32(&rf.state) != Leader {
				return nil, true
			}

			startIndex := rf.nextIndex[server]
			if logMatched {
				// 指数递增拷贝
				// nEntriesCopy = util.MinInt(nEntriesCopy*nEntriesCopy+1, len(rf.log)-startIndex)
				// 全部取出拷贝
				nEntriesCopy = util.MinInt(len(rf.log)-startIndex, config.KConf.BatchSize)
			} else {
				nEntriesCopy = util.MinInt(1, len(rf.log)-startIndex)
			}

			return &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: startIndex - 1,
				PrevLogTerm:  rf.log[startIndex-1].Term,
				Entries:      rf.log[startIndex : startIndex+nEntriesCopy],
				LeaderCommit: rf.commitIndex,
			}, false
		}()

		if stop {
			return
		}

		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| append entries to %d, entries=<%d, %d], PrevLogIndex=%d, PrevLogTerm=%d, LeaderCommit=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			server, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

		reply := &AppendEntriesReply{}
		before := time.Now()
		ok := rf.sendAppendEntries(server, args, reply)
		take := time.Since(before)

		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| append entries to %d, ok=%v, take=%v",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			server, ok, take)

		// 发送失败立刻重发
		if !ok {
			// logMatched = false
			continue
		}

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if atomic.LoadInt32(&rf.state) != Leader {
				return
			}

			// 超时后过期的回复消息
			if args.Term < rf.currentTerm {
				zlog.Trace("%d|%2d|%d|%d|<%d,%d>| append entries to %d, ok but expire, args.Term=%d, rf.currentTerm=%d, take=%v",
					rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
					server, args.Term, rf.currentTerm, take)
				return
			}

			// 保持连接置位
			atomic.StoreInt32(&rf.peers[server].connStatus, 1)

			// 遇到更高任期，成为follower
			if rf.currentTerm < reply.Term {
				zlog.Info("%d|%2d|%d|%d|<%d,%d>| append entries to %d, higher term, state:%s=>follower",
					rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
					server, state2str[atomic.LoadInt32(&rf.state)])
				rf.currentTerm = reply.Term
				atomic.StoreInt32(&rf.state, Follower)
				rf.leaderId = -1
				return
			}

			// 如果不成功，说明PrevLogIndex不匹配
			if !reply.Success {

				zlog.Debug("%d|%2d|%d|%d|<%d,%d>| append entries to %d, no match, PrevLogIndex=%d, PrevLogTerm=%d, reply.LogIndex=%d, reply.term=%d",
					rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
					server, args.PrevLogIndex, args.PrevLogTerm, reply.LogIndex, reply.Term)

				logMatched = rf.matchNextIndex(server, args.PrevLogIndex, args.PrevLogTerm, reply.LogIndex, reply.Term)
				return
			}

			if len(args.Entries) != 0 {
				// 复制到副本成功
				rf.matchIndex[server] = util.MaxInt(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}

			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| append entries to %d, match ok, copy entries=<%d, %d], match.index=%d, next.index=%d",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				server, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), rf.matchIndex[server], rf.nextIndex[server])

			// 本server commitIndex的值
			commitIndex = util.MinInt(args.LeaderCommit, rf.matchIndex[server])

			// 推进leader的commit
			rf.advanceLeaderCommit()
			// follower和leader日志匹配成功，可以发送后续日志
			logMatched = true
		}()
	}
}

func (rf *Raft) matchNextIndex(server, prevLogIndex, prevLogTerm, replyLogIndex, replyLogTerm int) bool {

	if rf.log[replyLogIndex].Term == replyLogTerm {
		// 刚好匹配，从下一个开始
		rf.nextIndex[server] = replyLogIndex + 1
		return true
	} else if rf.log[replyLogIndex].Term < replyLogTerm {
		// 从前面试起
		rf.nextIndex[server] = replyLogIndex
		return false
	}
	// 二分优化快速查找
	left, right := rf.matchIndex[server], replyLogIndex
	for left < right {
		mid := left + (right-left)/2
		zlog.Trace("%d|%2d|%d|%d|<%d,%d>| append entries to %d, for match, rf.log[%d].Term=%d, reply.Term=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			server, mid, rf.log[mid].Term, replyLogTerm)
		if rf.log[mid].Term <= replyLogTerm {
			left = mid + 1
		} else {
			right = mid
		}
	}
	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| append entries to %d, for match, rf.log[%d].Term=%d, reply.Term=%d",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		server, left, rf.log[left].Term, replyLogTerm)

	// 最近位置索引大一 (left - 1) + 1
	rf.nextIndex[server] = left
	return false
}

func (rf *Raft) advanceLeaderCommit() {
	// 判断是否需要递增 commitIndex，排序找出各个匹配的中位值就是半数以上都接受的日志
	indexes := make([]int, 0, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			indexes = append(indexes, rf.matchIndex[i])
		}
	}
	sort.Ints(indexes)
	newCommitIndex := indexes[len(indexes)-len(rf.peers)/2]
	// 相同任期才允许apply，避免被commit日志被覆盖的情况
	if rf.log[newCommitIndex].Term == rf.currentTerm && newCommitIndex > rf.commitIndex {
		// apply
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| step commit: %d => %d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			rf.commitIndex, newCommitIndex)

		rf.applyLogEntries(rf.commitIndex+1, newCommitIndex)
		rf.commitIndex = newCommitIndex
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ms := config.KConf.DelayFrom + rand.Intn(config.KConf.DelaRange)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	if err := rf.peers[server].rpcCli.Call("Raft.AppendEntries", args, reply); err != nil {
		zlog.Error("%v", err)
		return false
	}
	ms = config.KConf.DelayFrom + rand.Intn(config.KConf.DelaRange)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	return true
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term     int  // currentTerm, for leader to update itself
	LogIndex int  // 换主后，为leader快速定位匹配日志
	Success  bool // success true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return nil
	}

	atomic.StoreInt32(&rf.state, Follower) // 无论原来状态是什么，状态更新为follower
	atomic.StoreInt32(&rf.leaderLost, 0)   // 重置超时flag
	rf.currentTerm = args.Term             // 新的Term应该更高

	// 新的leader产生
	if args.LeaderId != rf.leaderId {
		zlog.Info("%d|%2d|%d|%d|<%d,%d>| state:%s=>follower, new leader %d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			state2str[atomic.LoadInt32(&rf.state)], args.LeaderId)
		rf.leaderId = args.LeaderId
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	// leader 的心跳消息，直接返回
	if args.PrevLogIndex < 0 {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| heartbeat from %d, reply.Term=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.LeaderId, reply.Term)
		reply.LogIndex = -100
		return nil
	}

	// 相同日志未直接定位到，需多轮交互
	if !rf.foundSameLog(args, reply) {
		return nil
	}

	// 对日志进行操作
	rf.updateEntries(args, reply)
	return nil
}

func (rf *Raft) foundSameLog(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if len(rf.log) <= args.PrevLogIndex {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| from %d, no found same log, len(rf.log)=%d <= args.PrevLogIndex=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.LeaderId, len(rf.log), args.PrevLogIndex)
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| from %d, no found same log, rf.log[%d].Term=%d != args.PrevLogTerm=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
	} else {
		return true
	}

	index := util.MinInt(args.PrevLogIndex, len(rf.log)-1)
	if rf.log[index].Term > args.PrevLogTerm {
		// 如果term不等，则采用二分查找找到最近匹配的日志索引
		left, right := rf.commitIndex, index
		for left < right {
			mid := left + (right-left)/2
			zlog.Trace("%d|%2d|%d|%d|<%d,%d>| from %d, no found same log, rf.log[%d].Term=%d, args.PrevLogTerm=%d",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				args.LeaderId, mid, rf.log[mid].Term, args.PrevLogTerm)
			if rf.log[mid].Term <= args.PrevLogTerm {
				left = mid + 1
			} else {
				right = mid
			}
		}
		index = left - 1
	}

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| from %d, no found same log, rf.log[%d].Term=%d, args.PrevLogTerm=%d",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		args.LeaderId, index, rf.log[index].Term, args.PrevLogTerm)

	reply.Term = rf.log[index].Term
	reply.LogIndex = index
	reply.Success = false
	return false
}

func (rf *Raft) updateEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 截断后面的日志
	if len(rf.log) > args.PrevLogIndex+1 {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| from %d, truncate log, rf.len: %d => %d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.LeaderId, len(rf.log), args.PrevLogIndex+1)
		rf.log = rf.log[:args.PrevLogIndex+1]
	}

	// 添加日志
	rf.log = append(rf.log, args.Entries...)

	// 在log变更时进行持久化
	// rf.persist()

	// 推进commit和apply
	oldCommitIndex := rf.commitIndex
	rf.commitIndex = util.MinInt(args.LeaderCommit, len(rf.log)-1)
	rf.applyLogEntries(oldCommitIndex+1, rf.commitIndex)
	rf.lastApplied = rf.commitIndex

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| from %d, append %d entries: <%d, %d], commit: <%d, %d]",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		args.LeaderId, len(args.Entries), args.PrevLogIndex, len(rf.log)-1, oldCommitIndex, rf.commitIndex)
}

func (rf *Raft) applyLogEntries(left, right int) {
	for i := left; i <= right; i++ {
		zlog.Trace("%d|%2d|%d|%d|<%d,%d>| from %d, apply, commandIndex=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			i, rf.commitIndex)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
	}
}

type RequestArgs struct {
	Cmd interface{}
}

type RequestReply struct {
	Index int
	Term  int
	Ok    bool
}

func (rf *Raft) Request(args *RequestArgs, reply *RequestReply) error {

	leaderId := func() int {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.me == rf.leaderId {
			reply.Index, reply.Term, reply.Ok = rf.start(args.Cmd)
			return -1
		}
		if rf.leaderId == -1 {
			reply.Ok = false
			return -1
		}
		return rf.leaderId
	}()

	if leaderId == -1 {
		return nil
	}

	return rf.peers[leaderId].rpcCli.Call("Raft.Request", args, reply)
}

func (rf *Raft) start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if atomic.LoadInt32(&rf.state) == Leader {
		isLeader = true
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		index = len(rf.log) - 1
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| new log, term=%d, index=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			term, index)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

		// rf.persist()
	}

	return index, term, isLeader
}

func (rf *Raft) getState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if atomic.LoadInt32(&rf.state) == Leader {
		isleader = true
	}

	return term, isleader
}

func (rf *Raft) applyLog() {

	outCh := make(chan []interface{}, 1000)
	go rf.persist(outCh)
	// statCh := make(chan interface{}, 1000)
	// go rf.stat(statCh)

	cmds := make([]interface{}, 0, config.KConf.EpochSize)
	t0 := time.Now()
	nApply := 0
	for msg := range rf.applyCh {
		nApply++
		cmds = append(cmds, msg.Command)
		if len(cmds) == cap(cmds) {
			tps := float64(config.KConf.EpochSize) / util.ToSecond(time.Since(t0))
			zlog.Info("%d|%2d|%d|%d|<%d,%d>| apply=%d, tps=%.2f",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				nApply, tps)
			outCh <- cmds
			// statCh <- strconv.Itoa(int(tps)) + " "
			cmds = make([]interface{}, 0, config.KConf.EpochSize)
			t0 = time.Now()
		}
	}
}

func (rf *Raft) persist(outCh chan []interface{}) {
	if !config.KConf.Persisted {
		for {
			<-outCh
		}
	}
	t0 := time.Now()
	path := fmt.Sprintf("%s/%02d_%s.log", config.KConf.LogDir, rf.me, t0.Format("2006-01-02_15:04:05"))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		zlog.Error("open log file fail, %v", err)
	}
	defer file.Close()
	write := bufio.NewWriter(file)
	for cmds := range outCh {
		for _, cmd := range cmds {
			write.WriteString(cmd.(string))
		}
	}
}

func (rf *Raft) stat(statCh chan interface{}) {
	t0 := time.Now()
	path := fmt.Sprintf("tps_%s.log", t0.Format("2006-01-02_15:04:05"))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		zlog.Error("open log file fail, %v", err)
	}
	defer file.Close()
	write := bufio.NewWriter(file)
	for st := range statCh {
		write.WriteString(st.(string))
	}
}

func (rf *Raft) test() {
	// reqTime := 5.0 // 请求时间

	return
	reqCount := 0
	format := fmt.Sprintf("%2d-%%-%dd\n", rf.me, config.KConf.ReqSize-4)

	k := util.MaxInt(10, 10*config.KConf.BatchSize/config.KConf.EpochSize)
	for {
		time.Sleep(intervalTime * time.Millisecond)
		if atomic.LoadInt32(&rf.state) == Leader {
			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| test start, reqCount=%d",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				reqCount)
		}

		for atomic.LoadInt32(&rf.state) == Leader {
			reqCount++
			rf.start(fmt.Sprintf(format, reqCount))
			for reqCount > (k*config.KConf.EpochSize)+rf.commitIndex {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}
