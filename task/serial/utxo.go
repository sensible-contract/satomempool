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
}

func CleanUtxoMap() {
	GlobalNewUtxoDataMap = make(map[string]*model.TxoData, 0)

	runtime.GC()
}
