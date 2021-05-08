package parser

import (
	"log"
	"satomempool/loader"
	"satomempool/model"
	"satomempool/utils"
	"sync"
	"time"
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

	mp.BlockNotify = make(chan []byte)
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

		tx, txoffset := NewTx(rawtx)
		if int(txoffset) < len(rawtx) {
			log.Println("rawtx decode failed")
			continue
		}

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
func (mp *Mempool) SyncMempoolFromZmq() {
	start := time.Now()

	firstGot := false
	for rawtx := range mp.RawTxNotify {
		if !firstGot {
			start = time.Now()
		}
		firstGot = true

		tx, txoffset := NewTx(rawtx)
		if int(txoffset) < len(rawtx) {
			continue
		}

		tx.Hash = utils.GetHash256(rawtx)
		tx.HashHex = utils.HashString(tx.Hash)

		if ok := mp.Txs[tx.HashHex]; ok {
			continue
		}

		mp.BatchTxs = append(mp.BatchTxs, tx)
		mp.Txs[tx.HashHex] = true

		if time.Since(start) > time.Second {
			return
		}
	}
}
