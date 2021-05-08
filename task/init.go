package task

import (
	"satomempool/logger"
	"satomempool/model"
	"satomempool/store"
	"satomempool/task/parallel"
	"satomempool/task/serial"
	"satomempool/task/utils"
)

var (
	MaxBlockHeightParallel int

	IsSync   bool
	IsDump   bool
	WithUtxo bool
	IsFull   bool
)

func init() {
	// serial.LoadUtxoFromGobFile()
}

// ParseBlockParallel 先并行分析区块，不同区块并行，同区块内串行
func ParseMempool(startIdx int, txs []*model.Tx,
	mpTokenSummary map[string]*model.TokenData,
	spentUtxoKeysMap map[string]bool,
	newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap map[string]*model.TxoData) {

	// first
	for txIdx, tx := range txs {
		parallel.ParseTxFirst(tx, mpTokenSummary)

		if WithUtxo {
			// 准备utxo花费关系数据
			parallel.ParseTxoSpendByTxParallel(tx, spentUtxoKeysMap)
			parallel.ParseNewUtxoInTxParallel(startIdx+txIdx, tx, newUtxoDataMap)
		}
	}

	if IsSync {
		serial.SyncBlockTxOutputInfo(txs)
	} else if IsDump {
		serial.DumpBlockTx(txs)
		serial.DumpBlockTxOutputInfo(txs)
		serial.DumpBlockTxInputInfo(txs)
	}

	// second
	utils.ParseBlockSpeed(len(txs), len(serial.GlobalNewUtxoDataMap))

	if WithUtxo {
		if IsSync {
			serial.ParseGetSpentUtxoDataFromRedisSerial(spentUtxoKeysMap, newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap)
			serial.SyncBlockTxInputDetail(txs, newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap, mpTokenSummary)

			serial.SyncBlockTx(txs)
		} else if IsDump {
			serial.DumpBlockTxInputDetail(txs, newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap)
		}

		// for txin dump
		serial.UpdateUtxoInRedisSerial(spentUtxoKeysMap, newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap)
	}

	// ParseEnd 最后分析执行
	if IsSync {
		store.CommitSyncCk()
		store.CommitFullSyncCk(serial.SyncTxFullCount > 0)
		store.ProcessPartSyncCk()
	}

	logger.SyncLog()
}
