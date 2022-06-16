package main

import (
	"flag"
	"zraft/zlog"
	"zraft/zraft"
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

	flag.IntVar(&zraft.KConf.EpochSize, "epochSize", 10000, "epoch size")
	flag.IntVar(&zraft.KConf.ReqSize, "reqSize", 128, "req size")
	flag.IntVar(&zraft.KConf.BatchSize, "batchSize", 1000, "batch size")
	flag.BoolVar(&zraft.KConf.Persisted, "persisted", false, "log out directory")
	flag.StringVar(&zraft.KConf.LogDir, "logDir", "./", "log out directory")
	flag.IntVar(&zraft.KConf.DelayFrom, "delayFrom", 0, "net delay from")
	flag.IntVar(&zraft.KConf.DelaRange, "delayRange", 0, "net delay range")

	flag.Parse()

	// 设置日志级别，默认为 Info(1)
	zlog.SetLevel(logLevel)

	// 根据不同身份启动节点
	switch role {
	case "master":
		// 执行命令: ./naive -role master -maddr localhost:8000 [-n 3]
		zraft.RunMaster(maddr, n)
	case "raft":
		// 执行命令: ./naive -role server -maddr localhost:8000 -saddr localhost:800x
		zraft.KConf.Show()
		zraft.RunRaft(maddr, saddr)
	case "client":
		// 执行命令: ./naive -role client -maddr localhost:8000 -caddr localhost:9000
		zraft.RunClient(maddr, caddr)
	default:
		flag.Usage()
	}
}
