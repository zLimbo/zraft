package draft

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"zraft/config"
	"zraft/util"
	"zraft/zlog"
)

type DRaft struct {
	Raft // combination
	// draft
	reqCh   chan interface{}
	draftCh chan []interface{}
	drafts  map[string][]interface{}
	nApply  int32
}

func RunDRaft(maddr, saddr string) {

	rf := &DRaft{
		Raft: Raft{
			addr:        saddr,
			state:       Follower,
			leaderId:    -1,
			applyCh:     make(chan ApplyMsg),
			currentTerm: 0,
			votedFor:    -1,
			log:         []LogEntry{{Term: 0}}, // 初始存一个默认entry，索引和任期为0
			commitIndex: 0,
			lastApplied: 0,
		},
		drafts:  make(map[string][]interface{}),
		reqCh:   make(chan interface{}, config.KConf.EpochSize),
		draftCh: make(chan []interface{}, 1000),
	}

	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| make DRaft, peers.num:%d",
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

	go rf.draft()

	select {}
}

func (rf *DRaft) applyLog() {

	outCh := make(chan []interface{}, 1000)
	go rf.persist(outCh)
	// statCh := make(chan interface{}, 1000)
	// go rf.stat(statCh)

	cmds := make([]interface{}, 0, config.KConf.EpochSize/config.KConf.BatchSize)
	t0 := time.Now()
	for msg := range rf.applyCh {
		// hash := msg.Command.(string)
		// ok := false
		// for !ok {
		// 	func() {
		// 		rf.mu.Lock()
		// 		defer rf.mu.Unlock()
		// 		if draft, ok := rf.drafts[hash]; ok {
		// 			atomic.AddInt32(&rf.nApply, int32(len(draft)))
		// 			ok = true
		// 		}
		// 	}()
		// 	time.Sleep(10 * time.Millisecond)
		// }
		// rf.mu.Lock()
		// if _, ok := rf.drafts[hash]; ok {
		// 	rf.drafts[hash] = nil
		// }
		// rf.mu.Unlock()
		atomic.AddInt32(&rf.nApply, int32(config.KConf.BatchSize))
		cmds = append(cmds, msg.Command)
		if len(cmds) == cap(cmds) {
			tps := float64(config.KConf.EpochSize) / util.ToSecond(time.Since(t0))
			zlog.Info("%d|%2d|%d|%d|<%d,%d>| apply=%d, tps=%.2f",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				rf.nApply, tps)
			outCh <- cmds
			// statCh <- strconv.Itoa(int(tps)) + " "
			cmds = make([]interface{}, 0, config.KConf.EpochSize/config.KConf.BatchSize)
			t0 = time.Now()
		}
	}
}

func (rf *DRaft) test() {
	// reqTime := 5.0 // 请求时间

	reqCount := 0
	format := fmt.Sprintf("%2d-%%-%dd\n", rf.me, config.KConf.ReqSize-4)

	// reqUpperBound := util.MaxInt(3, 3*config.KConf.BatchSize/config.KConf.EpochSize) * config.KConf.EpochSize
	for {
		time.Sleep(intervalTime * time.Millisecond)

		for rf.leaderId != -1 && atomic.LoadInt32(&rf.state) != Leader {
			reqCount++
			// rf.start(fmt.Sprintf(format, reqCount))
			rf.reqCh <- fmt.Sprintf(format, reqCount)
			// for reqCount > reqUpperBound+int(rf.nApply) {
			// 	time.Sleep(100 * time.Millisecond)
			// }
			if reqCount >= config.KConf.EpochNum*config.KConf.EpochSize {
				return
			}
		}
	}
}

func (rf *DRaft) draft() {

	go func() {
		reqs := make([]interface{}, 0, config.KConf.BatchSize)
		for req := range rf.reqCh {
			reqs = append(reqs, req)
			if len(reqs) == cap(reqs) {
				rf.draftCh <- reqs
				reqs = make([]interface{}, 0, config.KConf.BatchSize)
			}
		}
	}()

	nInc := 0
	func() {
		for draft := range rf.draftCh {
			nInc++
			hash := strconv.Itoa(nInc*100 + rf.me)
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.drafts[hash] = draft
			}()
			// TODO 先串行发
			rf.senddraftForAll(hash, draft)
		}
	}()
}

func (rf *DRaft) senddraftForAll(hash string, draft []interface{}) {
	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| send draft for all, hash=%s",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		hash)
	args := &SenddraftArgs{
		Id:    rf.me,
		Hash:  hash,
		draft: draft,
	}
	count := int32(1)
	up := int32(len(rf.peers)/2 + 1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		server1 := server
		go func() {
			reply := &SenddraftReply{}
			ok := rf.senddraft(server1, args, reply)
			zlog.Debug("%d|%2d|%d|%d|<%d,%d>| send draft => %d ok, hash=%s",
				rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
				server1, args.Hash)
			if ok && reply.Success {
				if atomic.AddInt32(&count, 1) == up {
					wg.Done()
					args := &SenddraftArgs{Id: rf.me, Hash: hash}
					reply := &SenddraftReply{}
					// todo 未加锁保护
					rf.senddraft(rf.leaderId, args, reply)
				}
			}
		}()
	}
	wg.Wait()
}

func (rf *DRaft) senddraft(server int, args *SenddraftArgs, reply *SenddraftReply) bool {
	if err := rf.peers[server].rpcCli.Call("DRaft.Senddraft", args, reply); err != nil {
		zlog.Error("%v", err)
		return false
	}
	return true
}

type SenddraftArgs struct {
	Id    int
	Hash  string
	draft []interface{}
}

type SenddraftReply struct {
	Success bool
}

func (rf *DRaft) Senddraft(args *SenddraftArgs, reply *SenddraftReply) error {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 处理哈希
	if len(args.draft) == 0 && atomic.LoadInt32(&rf.state) == Leader {
		zlog.Debug("%d|%2d|%d|%d|<%d,%d>| new draft <= %d, hash=%s, drafts.len=%d",
			rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
			args.Id, args.Hash, len(rf.drafts))
		go rf.start(args.Hash)
		reply.Success = true
		return nil
	}
	zlog.Debug("%d|%2d|%d|%d|<%d,%d>| send draft <= %d, hash=%s, drafts.len=%d",
		rf.me, rf.leaderId, rf.currentTerm, rf.commitIndex, len(rf.log)-1, rf.log[len(rf.log)-1].Term,
		args.Id, args.Hash, len(rf.drafts))
	if _, ok := rf.drafts[args.Hash]; !ok {
		rf.drafts[args.Hash] = args.draft
	}
	reply.Success = true
	return nil
}
