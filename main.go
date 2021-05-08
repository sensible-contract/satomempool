package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"satomempool/loader"
	"satomempool/parser"
	"satomempool/store"
	"satomempool/task"
	"satomempool/task/serial"
	"time"

	"github.com/spf13/viper"
)

var (
	zmqEndpoint    string
	listen_address = os.Getenv("LISTEN")
)

func init() {
	flag.BoolVar(&task.IsDump, "dump", false, "dump to file")
	flag.BoolVar(&task.IsSync, "sync", false, "sync to db")
	flag.BoolVar(&task.IsFull, "full", false, "start from genesis")
	flag.BoolVar(&task.WithUtxo, "utxo", true, "with utxo")

	flag.Parse()

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
	mempool, err := parser.NewMempool()
	if err != nil {
		log.Printf("init chain error: %v", err)
		return
	}

	// 监听新块确认
	go func() {
		loader.ZmqNotify(zmqEndpoint, mempool.RawTxNotify, mempool.BlockNotify)
	}()

	// "0.0.0.0:8080"
	server := &http.Server{Addr: listen_address, Handler: nil}

	startIdx := 0
	// 扫描区块
	go func() {
		for {
			mempool.Init()

			if task.IsFull {
				log.Printf("full sync...")
				startIdx = 0
				serial.CleanUtxoMap()
				serial.FlushdbInRedis()

				// 重新全量扫描
				mempool.LoadFromMempool()

				// 删除mempool数据
				store.ProcessAllSyncCk()
			} else {
				log.Printf("sync...")
				// 现有追加扫描
				mempool.SyncMempoolFromZmq()
			}

			// 初始化同步数据库表
			store.CreatePartSyncCk()
			store.PreparePartSyncCk()

			// 开始扫描区块，包括start，不包括end
			task.ParseMempool(
				startIdx,
				mempool.BatchTxs,
				mempool.TokenSummaryMap,
				mempool.SpentUtxoKeysMap,
				mempool.NewUtxoDataMap,
				mempool.SpentUtxoDataMap,
				mempool.RemoveUtxoDataMap,
			)

			startIdx += len(mempool.BatchTxs)

			log.Printf("finished")
			// 扫描完毕

			if task.IsSync {
				// 等待新块出现，再重新追加扫描
				task.IsFull = false
				select {
				case <-mempool.BlockNotify:
					task.IsFull = true
					log.Printf("new block...")
				default:
				}
			} else {
				// 结束
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				server.Shutdown(ctx)
			}
		}
	}()

	go func() {
		for {
			runtime.GC()
			time.Sleep(time.Second * 10)
		}
	}()

	// go tool pprof http://localhost:8080/debug/pprof/profile
	if err := server.ListenAndServe(); err != nil {
		log.Printf("profile listen failed: %v", err)
	}
}
