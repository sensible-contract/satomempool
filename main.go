package main

import (
	"fmt"
	"log"
	_ "net/http/pprof"
	"runtime"
	"satomempool/loader"
	"satomempool/store"
	"satomempool/task"
	"satomempool/task/serial"
	"time"

	"github.com/spf13/viper"
)

var (
	zmqEndpoint string
)

func init() {
	viper.SetConfigFile("conf/chain.yaml")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		} else {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	}

	zmqEndpoint = viper.GetString("zmq")
}

func main() {
	mempool, err := task.NewMempool()
	if err != nil {
		log.Printf("init chain error: %v", err)
		return
	}

	// 监听新块确认
	go func() {
		loader.ZmqNotify(zmqEndpoint, mempool.RawTxNotify)
	}()

	go func() {
		for {
			runtime.GC()
			time.Sleep(time.Second * 10)
		}
	}()

	startIdx := 0
	isFull := true
	// 扫描区块
	for {
		mempool.Init()

		if isFull {
			log.Printf("full sync...")
			startIdx = 0
			serial.CleanUtxoMap()
			serial.FlushdbInRedis()

			// 重新全量同步
			mempool.LoadFromMempool()

			// 删除mempool数据
			store.ProcessAllSyncCk()
		} else {
			log.Printf("sync...")
			// 现有追加同步
			if blockReady := mempool.SyncMempoolFromZmq(); blockReady {
				isFull = true
				continue
			}
		}

		// 初始化同步数据库表
		store.CreatePartSyncCk()
		store.PreparePartSyncCk()

		// 开始同步mempool
		mempool.ParseMempool(startIdx)

		startIdx += len(mempool.BatchTxs)

		// 同步完毕
		log.Printf("%d finished. +%d", startIdx, len(mempool.BatchTxs))

		isFull = false
	}
}
