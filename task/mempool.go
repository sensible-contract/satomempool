package task

import (
	"satomempool/loader"
	"satomempool/logger"
	"satomempool/model"
	"satomempool/store"
	"satomempool/task/parallel"
	"satomempool/task/serial"
	"satomempool/utils"
	"sync"
	"time"

	"go.uber.org/zap"
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
			logger.Log.Info("rawtx decode failed")
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
		case msg := <-serial.ChannelBlockSynced:
			loop := true
			nblk := 1
			for loop {
				select {
				case <-serial.ChannelBlockSynced:
					nblk++
					loop = true
				case <-time.After(time.Second):
					loop = false
				}
			}
			logger.Log.Info("redis subcribe", zap.Int("nblk", nblk), zap.String("channel", msg.Channel))
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
			logger.Log.Info("skip bad rawtx")
			continue
		}

		tx.Raw = rawtx
		tx.Size = uint32(txoffset)
		tx.Hash = utils.GetHash256(rawtx)
		tx.HashHex = utils.HashString(tx.Hash)

		if ok := mp.Txs[tx.HashHex]; ok {
			logger.Log.Info("skip dup")
			continue
		}

		mp.BatchTxs = append(mp.BatchTxs, tx)
		mp.Txs[tx.HashHex] = true

		if time.Since(start) > 200*time.Millisecond {
			return false
		}
	}
}

// ParseMempool 先并行分析区块，不同区块并行，同区块内串行
func (mp *Mempool) ParseMempool(startIdx int) {
	// first
	for txIdx, tx := range mp.BatchTxs {
		parallel.ParseTxFirst(tx)

		// 准备utxo花费关系数据
		parallel.ParseTxoSpendByTxParallel(tx, mp.SpentUtxoKeysMap)
		parallel.ParseNewUtxoInTxParallel(startIdx+txIdx, tx, mp.NewUtxoDataMap)
	}

	serial.SyncBlockTxOutputInfo(startIdx, mp.BatchTxs)

	serial.ParseGetSpentUtxoDataFromRedisSerial(mp.SpentUtxoKeysMap, mp.NewUtxoDataMap, mp.RemoveUtxoDataMap, mp.SpentUtxoDataMap)
	serial.SyncBlockTxInputDetail(startIdx, mp.BatchTxs, mp.NewUtxoDataMap, mp.RemoveUtxoDataMap, mp.SpentUtxoDataMap)

	serial.SyncBlockTx(startIdx, mp.BatchTxs)
	// for txin dump
	serial.UpdateUtxoInRedisSerial(mp.SpentUtxoKeysMap, mp.NewUtxoDataMap, mp.RemoveUtxoDataMap, mp.SpentUtxoDataMap)

	// ParseEnd 最后分析执行
	store.CommitSyncCk()
	store.CommitFullSyncCk(serial.SyncTxFullCount > 0)
	store.ProcessPartSyncCk()

	logger.SyncLog()
}
