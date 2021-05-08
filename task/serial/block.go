package serial

import (
	"satomempool/logger"
	"satomempool/model"

	"go.uber.org/zap"
)

// DumpBlockTx all tx in block height
func DumpBlockTx(txs []*model.Tx) {
	for idx, tx := range txs {
		logger.LogTx.Info("tx-list",
			zap.Binary("txid", tx.Hash),
			zap.Uint32("nTxIn", tx.TxInCnt),
			zap.Uint32("nTxOut", tx.TxOutCnt),
			zap.Uint32("size", tx.Size),
			zap.Uint32("locktime", tx.LockTime),
			zap.Uint32("height", uint32(model.MEMPOOL_HEIGHT)),
			zap.Uint64("idx", uint64(idx)),
		)
	}
}

// DumpBlockTxOutputInfo all tx output info
func DumpBlockTxOutputInfo(txs []*model.Tx) {
	for _, tx := range txs {
		for _, output := range tx.TxOuts {
			// if output.Value == 0 || !output.LockingScriptMatch {
			// 	continue
			// }

			logger.LogTxOut.Info("tx-txo",
				zap.Binary("utxoPoint", output.Outpoint),     // 36 bytes
				zap.ByteString("address", output.AddressPkh), // 20 bytes
				zap.ByteString("codehash", output.CodeHash),  // 20 bytes
				zap.ByteString("genesis", output.GenesisId),  // 20/36/40 bytes
				zap.Uint64("satoshi", output.Satoshi),
				zap.ByteString("scriptType", output.LockingScriptType),
				zap.ByteString("script", output.Pkscript),
				zap.Uint32("height", uint32(model.MEMPOOL_HEIGHT)),
			)
		}
	}
}

// DumpBlockTxInputInfo all tx input info
func DumpBlockTxInputInfo(txs []*model.Tx) {
	for _, tx := range txs {
		for _, input := range tx.TxIns {
			logger.LogTxIn.Info("tx-input",
				zap.Binary("txidIdx", input.InputPoint),
				zap.Binary("utxoPoint", input.InputOutpoint),
				zap.ByteString("scriptSig", input.ScriptSig),
				zap.Uint32("sequence", uint32(input.Sequence)),
				zap.Uint32("height", uint32(model.MEMPOOL_HEIGHT)),
			)
		}
	}
}

// DumpBlockTxInputDetail all tx input info
func DumpBlockTxInputDetail(txs []*model.Tx, newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap map[string]*model.TxoData) {
	var commonObjData *model.TxoData = &model.TxoData{
		CodeHash:   make([]byte, 1),
		GenesisId:  make([]byte, 1),
		AddressPkh: make([]byte, 1),
	}

	for _, tx := range txs {
		for inputIndex, input := range tx.TxIns {
			objData := commonObjData
			if obj, ok := newUtxoDataMap[input.InputOutpointKey]; ok {
				objData = obj
			} else if obj, ok := removeUtxoDataMap[input.InputOutpointKey]; ok {
				objData = obj
			} else if obj, ok := spentUtxoDataMap[input.InputOutpointKey]; ok {
				objData = obj
			} else {
				logger.Log.Info("tx-input-err",
					zap.String("txin", "input missing utxo"),
					zap.String("txid", tx.HashHex),
					zap.Int("idx", inputIndex),

					zap.String("utxid", input.InputHashHex),
					zap.Uint32("vout", input.InputVout),
				)
			}
			logger.LogTxIn.Info("tx-input-detail",
				zap.Uint32("height", uint32(model.MEMPOOL_HEIGHT)),
				zap.Binary("txidIdx", input.InputPoint),
				zap.ByteString("scriptSig", input.ScriptSig),
				zap.Uint32("sequence", uint32(input.Sequence)),

				zap.Uint32("height_out", uint32(objData.BlockHeight)),
				zap.Binary("utxoPoint", input.InputOutpoint),
				zap.ByteString("address", objData.AddressPkh), // 20 byte
				zap.ByteString("codehash", objData.CodeHash),  // 20 byte
				zap.ByteString("genesis", objData.GenesisId),  // 20 byte
				zap.Uint64("dataValue", objData.DataValue),
				zap.Uint64("satoshi", objData.Satoshi),
				zap.ByteString("scriptType", objData.ScriptType),
				zap.ByteString("script", objData.Script),
			)
		}
	}
}
