package main

import (
	"flag"
	"zraft/config"
	"zraft/draft"
	"zraft/master"
	"zraft/raft"
	"zraft/zlog"
)

func main() {

	// 解析命令行参数
	var role, maddr, saddr, caddr string
	var n, logLevel int
	flag.StringVar(&role, "role", "", "role of process")
	flag.StringVar(&maddr, "maddr", "", "address of master")
	flag.StringVar(&saddr, "saddr", "", "address of server")
	flag.StringVar(&caddr, "caddr", "", "address of client")
	flag.IntVar(&n, "n", 3, "peer num")
	flag.IntVar(&logLevel, "log", 2, "log level")

	flag.IntVar(&config.KConf.EpochSize, "epochSize", 10000, "epoch size")
	flag.IntVar(&config.KConf.ReqSize, "reqSize", 128, "req size")
	flag.IntVar(&config.KConf.EpochNum, "epochNum", 100, "epoch num")
	flag.IntVar(&config.KConf.BatchSize, "batchSize", 1000, "batch size")
	flag.BoolVar(&config.KConf.Persisted, "persisted", false, "log out directory")
	flag.StringVar(&config.KConf.LogDir, "logDir", "./", "log out directory")
	flag.IntVar(&config.KConf.DelayFrom, "delayFrom", 0, "net delay from")
	flag.IntVar(&config.KConf.DelayRange, "delayRange", 0, "net delay range")
	flag.BoolVar(&config.KConf.Draft, "draft", false, "use draft")

	flag.Parse()

	// 设置日志级别，默认为 Info(1)
	zlog.SetLevel(logLevel)

	// 根据不同身份启动节点
	switch role {
	case "master":
		// 执行命令: ./naive -role master -maddr localhost:8000 [-n 3]
		master.RunMaster(maddr, n)
	case "raft":
		// 执行命令: ./naive -role server -maddr localhost:8000 -saddr localhost:800x
		config.KConf.Show()
		// 选择使用 raft 还是 draft
		if config.KConf.Draft {
			draft.RunRaft(maddr, saddr)
		} else {
			raft.RunRaft(maddr, saddr)
		}

	case "client":
		// 执行命令: ./naive -role client -maddr localhost:8000 -caddr localhost:9000
		// client.RunClient(maddr, caddr)
	default:
		flag.Usage()
	}
}
