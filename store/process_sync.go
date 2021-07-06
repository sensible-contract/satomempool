package store

import (
	"satomempool/loader/clickhouse"
	"satomempool/logger"

	"go.uber.org/zap"
)

var (
	processAllSQLs = []string{
		// 删除mempool数据
		"ALTER TABLE blktx_height DELETE WHERE height >= 4294967295",
		"ALTER TABLE txin_spent DELETE WHERE height >= 4294967295",
		"ALTER TABLE txin DELETE WHERE height >= 4294967295",
		"ALTER TABLE txout DELETE WHERE height >= 4294967295",
	}

	createPartSQLs = []string{
		"DROP TABLE IF EXISTS blktx_height_mempool_new",
		"DROP TABLE IF EXISTS txout_mempool_new",
		"DROP TABLE IF EXISTS txin_mempool_new",

		"CREATE TABLE IF NOT EXISTS blktx_height_mempool_new AS blktx_height",
		"CREATE TABLE IF NOT EXISTS txout_mempool_new AS txout",
		"CREATE TABLE IF NOT EXISTS txin_mempool_new AS txin",
	}

	// 更新现有基础数据表blk_height、blktx_height、txin、txout
	processPartSQLsForTxIn = []string{
		"INSERT INTO txin SELECT * FROM txin_mempool_new",
		// 更新txo被花费的tx索引
		"INSERT INTO txin_spent SELECT height, txid, idx, utxid, vout FROM txin_mempool_new",

		"DROP TABLE IF EXISTS txin_mempool_new",
	}
	processPartSQLsForTxOut = []string{
		"INSERT INTO txout SELECT * FROM txout_mempool_new;",

		"DROP TABLE IF EXISTS txout_mempool_new",
	}

	processPartSQLs = []string{
		"INSERT INTO blktx_height SELECT * FROM blktx_height_mempool_new;",

		"DROP TABLE IF EXISTS blktx_height_mempool_new",
	}
)

func ProcessAllSyncCk() bool {
	// if !ProcessSyncCk(createAllSQLs) {
	// 	return false
	// }
	return ProcessSyncCk(processAllSQLs)
}

func CreatePartSyncCk() bool {
	return ProcessSyncCk(createPartSQLs)
}

func ProcessPartSyncCk() bool {
	if !ProcessSyncCk(processPartSQLs) {
		return false
	}
	if !ProcessSyncCk(processPartSQLsForTxIn) {
		return false
	}
	return ProcessSyncCk(processPartSQLsForTxOut)
}

func ProcessSyncCk(processSQLs []string) bool {
	for _, psql := range processSQLs {
		partLen := len(psql)
		if partLen > 128 {
			partLen = 128
		}
		if _, err := clickhouse.CK.Exec(psql); err != nil {
			logger.Log.Info("sync exec err", zap.String("sql", psql[:partLen]), zap.Error(err))
			return false
		}
	}
	return true
}
