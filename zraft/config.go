package zraft

import "zraft/zlog"

type Config struct {
	EpochSize int
	ReqSize   int
	BatchSize int
	Persisted bool
	LogDir    string
}

var KConf Config

func (c *Config) Show() {
	zlog.Info("> EpochSize=%d", c.EpochSize)
	zlog.Info("> ReqSize=%d", c.ReqSize)
	zlog.Info("> BatchSize=%d", c.BatchSize)
	zlog.Info("> Persisted=%v", c.Persisted)
	zlog.Info("> LogDir=%s", c.LogDir)
}
