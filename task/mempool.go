package task

import (
	"log"
	"satomempool/loader"
	"satomempool/logger"
	"satomempool/model"
	"satomempool/store"
	"satomempool/task/parallel"
	"satomempool/task/serial"
	"satomempool/utils"
	"sync"
	"time"
)

var (
	IsSync   bool
	IsDump   bool
	WithUtxo bool
	IsFull   bool
)

type Mempool struct {
	BatchTxs []*model.Tx     // 所有Tx
	Txs      map[string]bool // 所有Tx

	BlockNotify chan []byte
	RawTxNotify chan []byte

	SpentUtxoKeysMap  map[string]bool
	SpentUtxoDataMap  map[string]*model.TxoData
	NewUtxoDataMap    map[string]*model.TxoData
	RemoveUtxoDataMap map[string]*model.TxoData
	TokenSummaryMap   map[string]*model.TokenData // key: CodeHash+GenesisId;  nft: CodeHash+GenesisId+tokenIdx

	m sync.Mutex
}

func NewMempool() (mp *Mempool, err error) {
	mp = new(Mempool)

	mp.BlockNotify = make(chan []byte, 10)
	mp.RawTxNotify = make(chan []byte, 1000)

	return
}

func (mp *Mempool) Init() {
	mp.Txs = make(map[string]bool, 0)
	mp.BatchTxs = make([]*model.Tx, 0)
	mp.SpentUtxoKeysMap = make(map[string]bool, 1)
	mp.SpentUtxoDataMap = make(map[string]*model.TxoData, 1)
	mp.NewUtxoDataMap = make(map[string]*model.TxoData, 1)
	mp.RemoveUtxoDataMap = make(map[string]*model.TxoData, 1)
	mp.TokenSummaryMap = make(map[string]*model.TokenData, 1) // key: CodeHash+GenesisId  nft: CodeHash+GenesisId+tokenIdx
}

func (mp *Mempool) LoadFromMempool() bool {
	// 清空
	for i := 0; i < 1000; i++ {
		select {
		case <-mp.RawTxNotify:
		default:
		}
	}

	txids := loader.GetRawMemPoolRPC()
	for _, txid := range txids {
		rawtx := loader.GetRawTxRPC(txid)
		if rawtx == nil {
			continue
		}

		tx, txoffset := utils.NewTx(rawtx)
		if int(txoffset) < len(rawtx) {
			log.Println("rawtx decode failed")
			continue
		}

		tx.Raw = rawtx
		tx.Size = uint32(txoffset)
		tx.Hash = utils.GetHash256(rawtx)
		tx.HashHex = utils.HashString(tx.Hash)

		if ok := mp.Txs[tx.HashHex]; ok {
			continue
		}

		mp.BatchTxs = append(mp.BatchTxs, tx)
		mp.Txs[tx.HashHex] = true
	}
	return true
}

// SyncMempoolFromZmq 从zmq同步tx
func (mp *Mempool) SyncMempoolFromZmq() (blockReady bool) {
	start := time.Now()
	firstGot := false
	rawtx := make([]byte, 0)
	for {
		timeout := false
		select {
		case rawtx = <-mp.RawTxNotify:
			if !firstGot {
				start = time.Now()
			}
			firstGot = true
		case msg := <-serial.SubcribeBlockSynced.Channel():
			log.Println("redis subcribe:", msg.Channel)
			log.Println("redissubcribe:", msg.Payload)
			blockReady = true
		case <-time.After(time.Second):
			timeout = true
		}

		if blockReady {
			return true
		}
		if timeout {
			if firstGot {
				return false
			} else {
				continue
			}
		}

		tx, txoffset := utils.NewTx(rawtx)
		if int(txoffset) < len(rawtx) {
			log.Println("skip bad rawtx")
			continue
		}

		tx.Raw = rawtx
		tx.Size = uint32(txoffset)
		tx.Hash = utils.GetHash256(rawtx)
		tx.HashHex = utils.HashString(tx.Hash)

		if ok := mp.Txs[tx.HashHex]; ok {
			log.Println("skip dup")
			continue
		}

		mp.BatchTxs = append(mp.BatchTxs, tx)
		mp.Txs[tx.HashHex] = true

		if time.Since(start) > time.Second {
			log.Println("after 1s")
			return false
		}
	}
}

// ParseMempool 先并行分析区块，不同区块并行，同区块内串行
func (mp *Mempool) ParseMempool(startIdx int) {
	// first
	for txIdx, tx := range mp.BatchTxs {
		parallel.ParseTxFirst(tx, mp.TokenSummaryMap)

		if WithUtxo {
			// 准备utxo花费关系数据
			parallel.ParseTxoSpendByTxParallel(tx, mp.SpentUtxoKeysMap)
			parallel.ParseNewUtxoInTxParallel(startIdx+txIdx, tx, mp.NewUtxoDataMap)
		}
	}

	if IsSync {
		serial.SyncBlockTxOutputInfo(startIdx, mp.BatchTxs)
	} else if IsDump {
		serial.DumpBlockTx(mp.BatchTxs)
		serial.DumpBlockTxOutputInfo(mp.BatchTxs)
		serial.DumpBlockTxInputInfo(mp.BatchTxs)
	}

	// second
	serial.ParseBlockSpeed(len(mp.BatchTxs), len(serial.GlobalNewUtxoDataMap))

	if WithUtxo {
		if IsSync {
			serial.ParseGetSpentUtxoDataFromRedisSerial(mp.SpentUtxoKeysMap, mp.NewUtxoDataMap, mp.RemoveUtxoDataMap, mp.SpentUtxoDataMap)
			serial.SyncBlockTxInputDetail(startIdx, mp.BatchTxs, mp.NewUtxoDataMap, mp.RemoveUtxoDataMap, mp.SpentUtxoDataMap, mp.TokenSummaryMap)

			serial.SyncBlockTx(startIdx, mp.BatchTxs)
		} else if IsDump {
			serial.DumpBlockTxInputDetail(mp.BatchTxs, mp.NewUtxoDataMap, mp.RemoveUtxoDataMap, mp.SpentUtxoDataMap)
		}

		// for txin dump
		serial.UpdateUtxoInRedisSerial(mp.SpentUtxoKeysMap, mp.NewUtxoDataMap, mp.RemoveUtxoDataMap, mp.SpentUtxoDataMap)
	}

	// ParseEnd 最后分析执行
	if IsSync {
		store.CommitSyncCk()
		store.CommitFullSyncCk(serial.SyncTxFullCount > 0)
		store.ProcessPartSyncCk()
	}

	logger.SyncLog()
}
