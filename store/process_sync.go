package store

import (
	"log"
	"satomempool/loader/clickhouse"
)

var (
	createAllSQLs = []string{
		// tx list
		// ================================================================
		// 区块包含的交易列表，分区内按区块高度height排序、索引。按blk height查询时可确定分区 (快)
		"DROP TABLE IF EXISTS blktx_height",
		`
CREATE TABLE IF NOT EXISTS blktx_height (
	txid         FixedString(32),
	nin          UInt32,
	nout         UInt32,
	txsize       UInt32,
	locktime     UInt32,
	invalue      UInt64,
	outvalue     UInt64,
	rawtx        String,
	height       UInt32,
	blkid        FixedString(32),
	txidx        UInt64
) engine=MergeTree()
ORDER BY (height, txid)
PARTITION BY intDiv(height, 2100)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// txout
		// ================================================================
		// 交易输出列表，分区内按交易txid+idx排序、索引，单条记录包括输出的各种细节。仅按txid查询时将遍历所有分区（慢）
		// 查询需附带height，可配合tx_height表查询
		"DROP TABLE IF EXISTS txout",
		`
CREATE TABLE IF NOT EXISTS txout (
	utxid        FixedString(32),
	vout         UInt32,
	address      String,
	codehash     String,
	genesis      String,
	data_value   UInt64,
	satoshi      UInt64,
	script_type  String,
	script_pk    String,
	height       UInt32,
	utxidx       UInt64
) engine=MergeTree()
ORDER BY (utxid, vout)
PARTITION BY intDiv(height, 2100)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// txin
		// ================================================================
		// 交易输入的outpoint列表，分区内按outpoint txid+idx排序、索引。用于查询某txo被哪个tx花费，需遍历所有分区（慢）
		// 查询需附带height，需配合txout_spent_height表查询
		"DROP TABLE IF EXISTS txin_spent",
		`
CREATE TABLE IF NOT EXISTS txin_spent (
	height       UInt32,
	txid         FixedString(32),
	idx          UInt32,
	utxid        FixedString(32),
	vout         UInt32
) engine=MergeTree()
ORDER BY (utxid, vout)
PARTITION BY intDiv(height, 2100)
SETTINGS storage_policy = 'prefer_nvme_policy'`,

		// 交易输入列表，分区内按交易txid+idx排序、索引，单条记录包括输入的各种细节。仅按txid查询时将遍历所有分区（慢）
		// 查询需附带height。可配合tx_height表查询
		"DROP TABLE IF EXISTS txin",
		`
CREATE TABLE IF NOT EXISTS txin (
	height       UInt32,
	txidx        UInt64,
	txid         FixedString(32),
	idx          UInt32,
	script_sig   String,
	nsequence    UInt32,

	height_txo   UInt32,
	utxidx       UInt64,
	utxid        FixedString(32),
	vout         UInt32,
	address      String,
	codehash     String,
	genesis      String,
	data_value   UInt64,
	satoshi      UInt64,
	script_type  String,
	script_pk    String
) engine=MergeTree()
ORDER BY (txid, idx)
PARTITION BY intDiv(height, 2100)
SETTINGS storage_policy = 'prefer_nvme_policy'`,
	}

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
		log.Println("sync exec:", psql[:partLen])
		if _, err := clickhouse.CK.Exec(psql); err != nil {
			log.Println("sync exec err", psql, err.Error())
			return false
		}
	}
	return true
}
