package serial

import (
	"context"
	"encoding/binary"
	"fmt"
	"satomempool/model"
	"satomempool/script"

	redis "github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
)

var (
	rdb      *redis.Client
	rdbBlock *redis.Client
	ctx      = context.Background()
)

func init() {
	viper.SetConfigFile("conf/redis.yaml")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		} else {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	}

	address := viper.GetString("address")
	password := viper.GetString("password")
	database := viper.GetInt("database")
	databaseBlock := viper.GetInt("database_block")
	dialTimeout := viper.GetDuration("dialTimeout")
	readTimeout := viper.GetDuration("readTimeout")
	writeTimeout := viper.GetDuration("writeTimeout")
	poolSize := viper.GetInt("poolSize")
	rdb = redis.NewClient(&redis.Options{
		Addr:         address,
		Password:     password,
		DB:           database,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		PoolSize:     poolSize,
	})

	rdbBlock = redis.NewClient(&redis.Options{
		Addr:         address,
		Password:     password,
		DB:           databaseBlock,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		PoolSize:     poolSize,
	})
}

// ParseGetSpentUtxoDataFromRedisSerial 同步从redis中查询所需utxo信息来使用。稍慢但占用内存较少
// 如果withMap=true，部分utxo信息在程序内存，missing的utxo将从redis查询。区块同步结束时会批量更新缓存的utxo到redis。
// 稍快但占用内存较多
func ParseGetSpentUtxoDataFromRedisSerial(
	spentUtxoKeysMap map[string]bool,
	newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap map[string]*model.TxoData) {

	pipe := rdbBlock.Pipeline()
	m := map[string]*redis.StringCmd{}
	needExec := false
	for key := range spentUtxoKeysMap {
		if _, ok := newUtxoDataMap[key]; ok {
			continue
		}

		if data, ok := GlobalNewUtxoDataMap[key]; ok {
			removeUtxoDataMap[key] = data
			continue
		}

		needExec = true
		m[key] = pipe.Get(ctx, key)
	}

	if !needExec {
		return
	}

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		panic(err)
	}
	for key, v := range m {
		res, err := v.Result()
		if err == redis.Nil {
			continue
		} else if err != nil {
			panic(err)
		}
		d := model.TxoDataPool.Get().(*model.TxoData)
		d.Unmarshal([]byte(res))

		// 补充数据
		d.ScriptType = script.GetLockingScriptType(d.Script)
		d.IsNFT, d.CodeHash, d.GenesisId, d.AddressPkh, d.DataValue = script.ExtractPkScriptForTxo([]byte(key[:32]), d.Script, d.ScriptType)

		spentUtxoDataMap[key] = d
	}
}

// UpdateUtxoInRedisSerial 顺序更新当前区块的utxo信息变化到redis
func UpdateUtxoInRedisSerial(
	spentUtxoKeysMap map[string]bool,
	newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap map[string]*model.TxoData) {

	insideTxo := make([]string, len(spentUtxoKeysMap))
	for key := range spentUtxoKeysMap {
		if data, ok := newUtxoDataMap[key]; !ok {
			continue
		} else {
			model.TxoDataPool.Put(data)
		}
		insideTxo = append(insideTxo, key)
	}
	for _, key := range insideTxo {
		delete(newUtxoDataMap, key)
	}

	for key, data := range newUtxoDataMap {
		GlobalNewUtxoDataMap[key] = data
	}

	for key, data := range removeUtxoDataMap {
		model.TxoDataPool.Put(data)
		delete(GlobalNewUtxoDataMap, key)
	}

	UpdateUtxoInRedis(newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap)
}

func FlushdbInRedis() {
	rdb.FlushDB(ctx)
}

// UpdateUtxoInRedis 批量更新redis utxo
func UpdateUtxoInRedis(utxoToRestore, utxoToRemove, utxoToSpend map[string]*model.TxoData) (err error) {
	pipe := rdb.Pipeline()
	for key, data := range utxoToRestore {
		buf := make([]byte, 20+len(data.Script))
		data.Marshal(buf)
		// redis全局utxo数据添加
		pipe.Set(ctx, key, buf, 0)
		// redis有序utxo数据添加
		score := float64(data.BlockHeight)*1000000000 + float64(data.TxIdx)
		if err := pipe.ZAdd(ctx, "mp:utxo", &redis.Z{Score: score, Member: key}).Err(); err != nil {
			panic(err)
		}

		if len(data.AddressPkh) < 20 || len(data.GenesisId) < 20 {
			continue
		}

		// redis有序address utxo数据添加
		if err := pipe.ZAdd(ctx, "mp:au"+string(data.AddressPkh), &redis.Z{Score: score, Member: key}).Err(); err != nil {
			panic(err)
		}

		// balance of address
		if err := pipe.ZIncrBy(ctx, "mp:balance", float64(data.Satoshi), string(data.AddressPkh)).Err(); err != nil {
			panic(err)
		}

		// redis有序genesis utxo数据添加
		if data.IsNFT {
			// nft:utxo
			nftId := make([]byte, 8)
			binary.LittleEndian.PutUint64(nftId, data.DataValue) // 8
			if err := pipe.ZAdd(ctx, "mp:nu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh),
				&redis.Z{Score: score, Member: key + string(nftId)}).Err(); err != nil {
				panic(err)
			}
			// nft:owners
			if err := pipe.ZIncrBy(ctx, "mp:no"+string(data.CodeHash)+string(data.GenesisId),
				1, string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			// nft:summary
			if err := pipe.ZIncrBy(ctx, "mp:ns"+string(data.AddressPkh),
				1, string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
		} else {
			// ft:utxo
			if err := pipe.ZAdd(ctx, "mp:fu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh),
				&redis.Z{Score: score, Member: key}).Err(); err != nil {
				panic(err)
			}
			// ft:balance
			if err := pipe.ZIncrBy(ctx, "mp:fb"+string(data.CodeHash)+string(data.GenesisId),
				float64(data.DataValue),
				string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			// ft:summary
			if err := pipe.ZIncrBy(ctx, "mp:fs"+string(data.AddressPkh),
				float64(data.DataValue),
				string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
		}
	}

	addrToRemove := make(map[string]bool, 1)
	tokenToRemove := make(map[string]bool, 1)
	for key, data := range utxoToRemove {
		// redis全局utxo数据清除
		pipe.Del(ctx, key)
		// redis有序utxo数据清除
		if err := pipe.ZRem(ctx, "mp:utxo", key).Err(); err != nil {
			panic(err)
		}

		if len(data.AddressPkh) < 20 || len(data.GenesisId) < 20 {
			continue
		}

		// redis有序address utxo数据清除
		if err := pipe.ZRem(ctx, "mp:au"+string(data.AddressPkh), key).Err(); err != nil {
			panic(err)
		}

		// balance of address
		if err := pipe.ZIncrBy(ctx, "mp:balance", -float64(data.Satoshi), string(data.AddressPkh)).Err(); err != nil {
			panic(err)
		}

		// redis有序genesis utxo数据清除
		if data.IsNFT {
			// nft:utxo
			nftId := make([]byte, 8)
			binary.LittleEndian.PutUint64(nftId, data.DataValue) // 8
			if err := pipe.ZRem(ctx, "mp:nu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh),
				key+string(nftId)).Err(); err != nil {
				panic(err)
			}

			// nft:owners
			if err := pipe.ZIncrBy(ctx, "mp:no"+string(data.CodeHash)+string(data.GenesisId),
				-1, string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			// nft:summary
			if err := pipe.ZIncrBy(ctx, "mp:ns"+string(data.AddressPkh),
				-1, string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
		} else {
			// ft:utxo
			if err := pipe.ZRem(ctx, "mp:fu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh), key).Err(); err != nil {
				panic(err)
			}
			// ft:balance
			if err := pipe.ZIncrBy(ctx, "mp:fb"+string(data.CodeHash)+string(data.GenesisId),
				-float64(data.DataValue),
				string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			// ft:summary
			if err := pipe.ZIncrBy(ctx, "mp:fs"+string(data.AddressPkh),
				-float64(data.DataValue),
				string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
		}

		// 记录key以备删除
		tokenToRemove[string(data.CodeHash)+string(data.GenesisId)] = true
		addrToRemove[string(data.AddressPkh)] = true
	}

	for key, data := range utxoToSpend {
		buf := make([]byte, 20+len(data.Script))
		data.Marshal(buf)
		// redis有序utxo数据添加
		score := float64(data.BlockHeight)*1000000000 + float64(data.TxIdx)
		if err := pipe.ZAdd(ctx, "mp:s:utxo", &redis.Z{Score: score, Member: key}).Err(); err != nil {
			panic(err)
		}

		if len(data.AddressPkh) < 20 || len(data.GenesisId) < 20 {
			continue
		}

		// redis有序address utxo数据添加
		if err := pipe.ZAdd(ctx, "mp:s:au"+string(data.AddressPkh), &redis.Z{Score: score, Member: key}).Err(); err != nil {
			panic(err)
		}

		// balance of address
		if err := pipe.ZIncrBy(ctx, "mp:balance", -float64(data.Satoshi), string(data.AddressPkh)).Err(); err != nil {
			panic(err)
		}

		// redis有序genesis utxo数据添加
		if data.IsNFT {
			// nft:utxo
			nftId := make([]byte, 8)
			binary.LittleEndian.PutUint64(nftId, data.DataValue) // 8
			if err := pipe.ZAdd(ctx, "mp:s:nu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh),
				&redis.Z{Score: score, Member: key + string(nftId)}).Err(); err != nil {
				panic(err)
			}
			// nft:owners
			if err := pipe.ZIncrBy(ctx, "mp:no"+string(data.CodeHash)+string(data.GenesisId),
				-1, string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			// nft:summary
			if err := pipe.ZIncrBy(ctx, "mp:ns"+string(data.AddressPkh),
				-1, string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
		} else {
			// ft:utxo
			if err := pipe.ZAdd(ctx, "mp:s:fu"+string(data.CodeHash)+string(data.GenesisId)+string(data.AddressPkh),
				&redis.Z{Score: score, Member: key}).Err(); err != nil {
				panic(err)
			}
			// ft:balance
			if err := pipe.ZIncrBy(ctx, "mp:fb"+string(data.CodeHash)+string(data.GenesisId),
				-float64(data.DataValue),
				string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			// ft:summary
			if err := pipe.ZIncrBy(ctx, "mp:fs"+string(data.AddressPkh),
				-float64(data.DataValue),
				string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
		}

		// 记录key以备删除
		tokenToRemove[string(data.CodeHash)+string(data.GenesisId)] = true
		addrToRemove[string(data.AddressPkh)] = true
	}

	// 删除summary 为0的记录
	for codeKey := range tokenToRemove {
		if err := pipe.ZRemRangeByScore(ctx, "mp:no"+codeKey, "0", "0").Err(); err != nil {
			panic(err)
		}
		if err := pipe.ZRemRangeByScore(ctx, "mp:fb"+codeKey, "0", "0").Err(); err != nil {
			panic(err)
		}
	}
	// 删除balance 为0的记录
	for addr := range addrToRemove {
		if err := pipe.ZRemRangeByScore(ctx, "mp:ns"+addr, "0", "0").Err(); err != nil {
			panic(err)
		}
		if err := pipe.ZRemRangeByScore(ctx, "mp:fs"+addr, "0", "0").Err(); err != nil {
			panic(err)
		}
	}
	// 删除balance 为0的记录
	if err := pipe.ZRemRangeByScore(ctx, "mp:balance", "0", "0").Err(); err != nil {
		panic(err)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		panic(err)
	}
	return nil
}
