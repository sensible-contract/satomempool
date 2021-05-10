package serial

import (
	"satomempool/logger"
	"satomempool/model"
	"satomempool/store"
	"strconv"

	"go.uber.org/zap"
)

var (
	SyncTxFullCount     int
	SyncTxCodeHashCount int
)

// SyncBlockTx all tx in block height
func SyncBlockTx(startIdx int, txs []*model.Tx) {
	for txIdx, tx := range txs {
		if _, err := store.SyncStmtTx.Exec(
			string(tx.Hash),
			tx.TxInCnt,
			tx.TxOutCnt,
			tx.Size,
			tx.LockTime,
			tx.InputsValue,
			tx.OutputsValue,
			model.MEMPOOL_HEIGHT, // uint32(block.Height),
			"",                   // string(block.Hash),
			uint64(startIdx+txIdx),
		); err != nil {
			logger.Log.Info("sync-tx-err",
				zap.String("sync", "tx err"),
				zap.String("txid", tx.HashHex),
				zap.String("err", err.Error()),
			)
		}
	}
}

// SyncBlockTxOutputInfo all tx output info
func SyncBlockTxOutputInfo(startIdx int, txs []*model.Tx) {
	for txIdx, tx := range txs {
		for vout, output := range tx.TxOuts {
			tx.OutputsValue += output.Satoshi

			if _, err := store.SyncStmtTxOut.Exec(
				string(tx.Hash),
				uint32(vout),
				string(output.AddressPkh), // 20 bytes
				string(output.CodeHash),   // 20 bytes
				string(output.GenesisId),  // 20/36/40 bytes
				output.DataValue,
				output.Satoshi,
				string(output.LockingScriptType),
				string(output.Pkscript),
				model.MEMPOOL_HEIGHT, // uint32(block.Height),
				uint64(startIdx+txIdx),
			); err != nil {
				logger.Log.Info("sync-txout-err",
					zap.String("sync", "txout err"),
					zap.String("utxid", tx.HashHex),
					zap.Uint32("vout", uint32(vout)),
					zap.String("err", err.Error()),
				)
			}
		}
	}
}

// SyncBlockTxInputDetail all tx input info
func SyncBlockTxInputDetail(startIdx int, txs []*model.Tx, mpNewUtxo, removeUtxo, mpSpentUtxo map[string]*model.TxoData, mpTokenSummary map[string]*model.TokenData) {
	var commonObjData *model.TxoData = &model.TxoData{
		CodeHash:   make([]byte, 1),
		GenesisId:  make([]byte, 1),
		AddressPkh: make([]byte, 1),
		Satoshi:    0,
	}

	for txIdx, tx := range txs {
		for vin, input := range tx.TxIns {
			objData := commonObjData
			if obj, ok := mpNewUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else if obj, ok := removeUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else if obj, ok := mpSpentUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else {
				logger.Log.Info("tx-input-err",
					zap.String("txin", "input missing utxo"),
					zap.String("txid", tx.HashHex),
					zap.Int("vin", vin),

					zap.String("utxid", input.InputHashHex),
					zap.Uint32("vout", input.InputVout),
				)
			}
			tx.InputsValue += objData.Satoshi

			// token summary
			if len(objData.CodeHash) == 20 && len(objData.GenesisId) >= 20 {
				NFTIdx := uint64(0)
				key := string(objData.CodeHash) + string(objData.GenesisId)
				if objData.IsNFT {
					key += strconv.Itoa(int(objData.DataValue))
					NFTIdx = objData.DataValue
				}

				tokenSummary, ok := mpTokenSummary[key]
				if !ok {
					tokenSummary = &model.TokenData{
						IsNFT:     objData.IsNFT,
						NFTIdx:    NFTIdx,
						CodeHash:  objData.CodeHash,
						GenesisId: objData.GenesisId,
					}
					mpTokenSummary[key] = tokenSummary
				}

				tokenSummary.InSatoshi += objData.Satoshi
				if objData.IsNFT {
					tokenSummary.InDataValue += 1
				} else {
					tokenSummary.InDataValue += objData.DataValue
				}
			}

			SyncTxFullCount++
			if _, err := store.SyncStmtTxIn.Exec(
				model.MEMPOOL_HEIGHT, // uint32(block.Height),
				uint64(startIdx+txIdx),
				string(tx.Hash),
				uint32(vin),
				string(input.ScriptSig),
				uint32(input.Sequence),

				uint32(objData.BlockHeight),
				uint64(objData.TxIdx),
				string(input.InputHash),
				input.InputVout,
				string(objData.AddressPkh), // 20 byte
				string(objData.CodeHash),   // 20 byte
				string(objData.GenesisId),  // 20 byte
				objData.DataValue,
				objData.Satoshi,
				string(objData.ScriptType),
				string(objData.Script),
			); err != nil {
				logger.Log.Info("sync-txin-full-err",
					zap.String("sync", "txin full err"),
					zap.String("txid", tx.HashHex),
					zap.Uint32("vin", uint32(vin)),
					zap.String("err", err.Error()),
				)
			}
		}
	}
}
