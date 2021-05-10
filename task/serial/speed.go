package serial

import (
	"satomempool/logger"
	"time"

	"go.uber.org/zap"
)

var (
	start            time.Time = time.Now()
	lastLogTime      time.Time
	lastBlockTxCount int
)

func ParseBlockSpeed(nTx, lenGlobalNewUtxoDataMap int) {
	lastBlockTxCount += nTx

	if time.Since(lastLogTime) < time.Second {
		return
	}

	lastLogTime = time.Now()

	logger.LogErr.Info("parsing",
		zap.Int("ntx", lastBlockTxCount),
		zap.Int("utxo", lenGlobalNewUtxoDataMap),
		zap.Duration("elapse", time.Since(start)/time.Second),
	)

	lastBlockTxCount = 0
}
