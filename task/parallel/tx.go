package parallel

import (
	"encoding/binary"
	"encoding/hex"
	"satomempool/model"

	script "github.com/sensible-contract/sensible-script-decoder"
)

// ParseTx 先并行分析交易tx，不同区块并行，同区块内串行
func ParseTxFirst(tx *model.Tx) {
	for idx, input := range tx.TxIns {
		key := make([]byte, 36)
		copy(key, tx.Hash)
		binary.LittleEndian.PutUint32(key[32:], uint32(idx))
		input.InputPoint = key
	}

	for idx, output := range tx.TxOuts {
		key := make([]byte, 36)
		copy(key, tx.Hash)

		binary.LittleEndian.PutUint32(key[32:], uint32(idx))
		output.OutpointKey = string(key)
		output.Outpoint = key

		output.LockingScriptType = script.GetLockingScriptType(output.Pkscript)
		output.LockingScriptTypeHex = hex.EncodeToString(output.LockingScriptType)

		// address
		output.IsNFT, output.CodeHash, output.GenesisId, output.AddressPkh, output.Name, output.Symbol, output.DataValue, output.Decimal = script.ExtractPkScriptForTxo(output.Pkscript, output.LockingScriptType)

		if script.IsOpreturn(output.LockingScriptType) {
			output.LockingScriptUnspendable = true
		}
	}
}

// ParseTxoSpendByTxParallel utxo被使用
func ParseTxoSpendByTxParallel(tx *model.Tx, spentUtxoKeysMap map[string]bool) {
	for _, input := range tx.TxIns {
		spentUtxoKeysMap[input.InputOutpointKey] = true
	}
}

// ParseNewUtxoInTxParallel utxo 信息
func ParseNewUtxoInTxParallel(txIdx int, tx *model.Tx, mpNewUtxo map[string]*model.TxoData) {
	for _, output := range tx.TxOuts {
		if output.LockingScriptUnspendable {
			continue
		}

		d := model.TxoDataPool.Get().(*model.TxoData)
		d.BlockHeight = model.MEMPOOL_HEIGHT
		d.TxIdx = uint64(txIdx)
		d.AddressPkh = output.AddressPkh
		d.IsNFT = output.IsNFT
		d.CodeHash = output.CodeHash
		d.GenesisId = output.GenesisId
		d.DataValue = output.DataValue
		d.Decimal = output.Decimal
		d.Name = output.Name
		d.Symbol = output.Symbol
		d.Satoshi = output.Satoshi
		d.ScriptType = output.LockingScriptType
		d.Script = output.Pkscript

		mpNewUtxo[output.OutpointKey] = d
	}
}
