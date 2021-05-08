package serial

import (
	"runtime"
	"satomempool/model"
)

var (
	GlobalNewUtxoDataMap map[string]*model.TxoData
)

func init() {
	CleanUtxoMap()
	// loadUtxoFromGobFile()
}

func CleanUtxoMap() {
	GlobalNewUtxoDataMap = make(map[string]*model.TxoData, 0)

	runtime.GC()
}
