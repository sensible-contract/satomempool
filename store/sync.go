package store

import (
	"database/sql"
	"fmt"
	"log"
	"satomempool/loader/clickhouse"
)

var (
	SyncStmtTx    *sql.Stmt
	SyncStmtTxOut *sql.Stmt
	SyncStmtTxIn  *sql.Stmt

	syncTxTx    *sql.Tx
	syncTxTxOut *sql.Tx
	syncTxTxIn  *sql.Tx

	sqlTxPattern    string = "INSERT INTO %s (txid, nin, nout, txsize, locktime, invalue, outvalue, rawtx, height, blkid, txidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxOutPattern string = "INSERT INTO %s (utxid, vout, address, codehash, genesis, data_value, satoshi, script_type, script_pk, height, utxidx) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	sqlTxInPattern  string = "INSERT INTO %s (height, txidx, txid, idx, script_sig, nsequence, height_txo, utxidx, utxid, vout, address, codehash, genesis, data_value, satoshi, script_type, script_pk) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

func prepareSyncCk() bool {
	sqlTx := fmt.Sprintf(sqlTxPattern, "blktx_height_mempool_new")
	sqlTxOut := fmt.Sprintf(sqlTxOutPattern, "txout_mempool_new")
	sqlTxIn := fmt.Sprintf(sqlTxInPattern, "txin_mempool_new")

	var err error

	syncTxTx, err = clickhouse.CK.Begin()
	if err != nil {
		log.Println("sync-begin-tx", err.Error())
		return false
	}
	SyncStmtTx, err = syncTxTx.Prepare(sqlTx)
	if err != nil {
		log.Println("sync-prepare-tx", err.Error())
		return false
	}

	syncTxTxOut, err = clickhouse.CK.Begin()
	if err != nil {
		log.Println("sync-begin-txout", err.Error())
		return false
	}
	SyncStmtTxOut, err = syncTxTxOut.Prepare(sqlTxOut)
	if err != nil {
		log.Println("sync-prepare-txout", err.Error())
		return false
	}

	syncTxTxIn, err = clickhouse.CK.Begin()
	if err != nil {
		log.Println("sync-begin-txinfull", err.Error())
		return false
	}
	SyncStmtTxIn, err = syncTxTxIn.Prepare(sqlTxIn)
	if err != nil {
		log.Println("sync-prepare-txinfull", err.Error())
		return false
	}

	return true
}

func PreparePartSyncCk() bool {
	return prepareSyncCk()
}

func CommitSyncCk() {
	defer SyncStmtTx.Close()
	defer SyncStmtTxOut.Close()

	if err := syncTxTx.Commit(); err != nil {
		log.Println("sync-commit-tx", err.Error())
	}
	if err := syncTxTxOut.Commit(); err != nil {
		log.Println("sync-commit-txout", err.Error())
	}
}

func CommitFullSyncCk(needCommit bool) {
	defer SyncStmtTxIn.Close()

	if !needCommit {
		syncTxTxIn.Rollback()
		return
	}

	if err := syncTxTxIn.Commit(); err != nil {
		log.Println("sync-commit-txinfull", err.Error())
	}
}
