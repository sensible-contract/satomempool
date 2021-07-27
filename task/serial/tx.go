package serial

import (
	"context"
	"encoding/hex"
	"fmt"
	"satomempool/logger"
	"satomempool/model"

	redis "github.com/go-redis/redis/v8"
	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	rdbc                *redis.ClusterClient
	ctx                 = context.Background()
	SubcribeBlockSynced *redis.PubSub
	ChannelBlockSynced  <-chan *redis.Message
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

	clusterAddrs := viper.GetStringSlice("clusterAddrs")
	password := viper.GetString("password")
	dialTimeout := viper.GetDuration("dialTimeout")
	readTimeout := viper.GetDuration("readTimeout")
	writeTimeout := viper.GetDuration("writeTimeout")
	poolSize := viper.GetInt("poolSize")
	rdbc = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        clusterAddrs,
		Password:     password,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		PoolSize:     poolSize,
	})

	SubcribeBlockSynced = rdbc.Subscribe(ctx, "channel_block_sync")
	ChannelBlockSynced = SubcribeBlockSynced.Channel()
}

func SubcribeBlockSyncFinished() {
	msg := <-ChannelBlockSynced
	logger.Log.Info("redis subcribe",
		zap.String("channel", msg.Channel),
		zap.String("payload", msg.Payload))
}

// ParseGetSpentUtxoDataFromRedisSerial 同步从redis中查询所需utxo信息来使用。稍慢但占用内存较少
// 如果withMap=true，部分utxo信息在程序内存，missing的utxo将从redis查询。区块同步结束时会批量更新缓存的utxo到redis。
// 稍快但占用内存较多
func ParseGetSpentUtxoDataFromRedisSerial(
	spentUtxoKeysMap map[string]bool,
	newUtxoDataMap, removeUtxoDataMap, spentUtxoDataMap map[string]*model.TxoData) {

	pipe := rdbc.Pipeline()
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
		d.ScriptType = scriptDecoder.GetLockingScriptType(d.Script)
		txo := scriptDecoder.ExtractPkScriptForTxo(d.Script, d.ScriptType)

		d.CodeType = txo.CodeType
		d.CodeHash = txo.CodeHash
		d.GenesisId = txo.GenesisId
		d.SensibleId = txo.SensibleId
		d.AddressPkh = txo.AddressPkh

		// nft
		d.MetaTxId = txo.MetaTxId
		d.MetaOutputIndex = txo.MetaOutputIndex
		d.TokenIndex = txo.TokenIndex
		d.TokenSupply = txo.TokenSupply

		// ft
		d.Name = txo.Name
		d.Symbol = txo.Symbol
		d.Amount = txo.Amount
		d.Decimal = txo.Decimal

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
	logger.Log.Info("FlushdbInRedis start")
	keys, err := rdbc.SMembers(ctx, "mp:keys").Result()
	if err != nil {
		logger.Log.Info("FlushdbInRedis redis failed", zap.Error(err))
		return
	}
	logger.Log.Info("FlushdbInRedis", zap.Int("nKeys", len(keys)))

	if len(keys) == 0 {
		return
	}
	pipe := rdbc.Pipeline()
	for _, key := range keys {
		pipe.Del(ctx, key)
	}
	pipe.Del(ctx, "mp:keys")
	_, err = pipe.Exec(ctx)
	if err != nil {
		panic(err)
	}
	logger.Log.Info("FlushdbInRedis finish")
}

// UpdateUtxoInRedis 批量更新redis utxo
func UpdateUtxoInRedis(utxoToRestore, utxoToRemove, utxoToSpend map[string]*model.TxoData) (err error) {
	logger.Log.Info("UpdateUtxoInRedis",
		zap.Int("nStore", len(utxoToRestore)),
		zap.Int("nRemove", len(utxoToRemove)),
		zap.Int("nSpend", len(utxoToSpend)))
	mpkey := ""
	pipe := rdbc.Pipeline()
	for key, data := range utxoToRestore {
		buf := make([]byte, 20+len(data.Script))
		data.Marshal(buf)
		// redis全局utxo数据添加
		// fixme: 会覆盖satoblock？
		pipe.SetNX(ctx, "u"+key, buf, 0)
		// redis有序utxo数据添加
		score := float64(data.BlockHeight)*1000000000 + float64(data.TxIdx)
		if len(data.AddressPkh) < 20 {
			// 无法识别地址，暂不记录utxo
			logger.Log.Info("ignore mp:utxo", zap.String("key", hex.EncodeToString([]byte(key))), zap.Float64("score", score))
			// logger.Log.Info("ZAdd mp:utxo", zap.String("key", hex.EncodeToString([]byte(key))), zap.Float64("score", score))
			// if err := pipe.ZAdd(ctx, "mp:utxo", &redis.Z{Score: score, Member: key}).Err(); err != nil {
			// 	panic(err)
			// }
			continue
		}

		if len(data.GenesisId) < 20 {
			// 不是合约tx，则记录address utxo
			// redis有序address utxo数据添加
			logger.Log.Info("ZAdd mp:au",
				zap.String("addrHex", hex.EncodeToString(data.AddressPkh)),
				zap.String("key", hex.EncodeToString([]byte(key))),
				zap.Float64("score", score))

			mpkey = "mp:{au" + string(data.AddressPkh) + "}"
			if err := pipe.ZAdd(ctx, mpkey, &redis.Z{Score: score, Member: key}).Err(); err != nil {
				panic(err)
			}

			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// balance of address
			logger.Log.Info("IncrBy mp:bl",
				zap.String("addrHex", hex.EncodeToString(data.AddressPkh)),
				zap.Uint64("satoshi", data.Satoshi))

			mpkey = "mp:bl" + string(data.AddressPkh)
			if err := pipe.IncrBy(ctx, mpkey, int64(data.Satoshi)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}
			continue
		}

		// contract balance of address
		logger.Log.Info("IncrBy mp:cb",
			zap.String("addrHex", hex.EncodeToString(data.AddressPkh)),
			zap.Uint64("satoshi", data.Satoshi))

		mpkey = "mp:cb" + string(data.AddressPkh)
		if err := pipe.IncrBy(ctx, mpkey, int64(data.Satoshi)).Err(); err != nil {
			panic(err)
		}
		if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
			panic(err)
		}

		// redis有序genesis utxo数据添加
		if data.CodeType == scriptDecoder.CodeType_NFT {
			logger.Log.Info("=== update nft")

			pipe.HSet(ctx, "ni"+string(data.CodeHash)+string(data.GenesisId),
				"metatxid", data.MetaTxId,
				"metavout", data.MetaOutputIndex,
				"supply", data.TokenSupply,
				"sensibleid", data.SensibleId,
			)

			nftId := float64(data.TokenIndex)
			// nft:utxo
			mpkey = "mp:{nu" + string(data.AddressPkh) + "}" + string(data.CodeHash) + string(data.GenesisId)
			if err := pipe.ZAdd(ctx, mpkey, &redis.Z{Score: nftId, Member: key}).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// nft:utxo-detail
			mpkey = "mp:nd" + string(data.CodeHash) + string(data.GenesisId)
			if err := pipe.ZAdd(ctx, mpkey, &redis.Z{Score: nftId, Member: key}).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// nft:owners
			mpkey = "mp:{no" + string(data.GenesisId) + string(data.CodeHash) + "}"
			if err := pipe.ZIncrBy(ctx, mpkey, 1, string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// nft:summary
			mpkey = "mp:{ns" + string(data.AddressPkh) + "}"
			if err := pipe.ZIncrBy(ctx, mpkey, 1, string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

		} else if data.CodeType == scriptDecoder.CodeType_FT {
			// ft:info
			logger.Log.Info("=== update ft")
			pipe.HSet(ctx, "fi"+string(data.CodeHash)+string(data.GenesisId),
				"decimal", data.Decimal,
				"name", data.Name,
				"symbol", data.Symbol,
				"sensibleid", data.SensibleId,
			)
			// ft:utxo
			mpkey = "mp:{fu" + string(data.AddressPkh) + "}" + string(data.CodeHash) + string(data.GenesisId)
			if err := pipe.ZAdd(ctx, mpkey, &redis.Z{Score: score, Member: key}).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// ft:balance
			mpkey = "mp:{fb" + string(data.GenesisId) + string(data.CodeHash) + "}"
			if err := pipe.ZIncrBy(ctx, mpkey, float64(data.Amount), string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// ft:summary
			mpkey = "mp:{fs" + string(data.AddressPkh) + "}"
			if err := pipe.ZIncrBy(ctx, mpkey, float64(data.Amount), string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

		} else if data.CodeType == scriptDecoder.CodeType_UNIQUE {
			// ft:info
			logger.Log.Info("=== update unique")
			pipe.HSet(ctx, "fi"+string(data.CodeHash)+string(data.GenesisId),
				"decimal", data.Decimal,
				"name", data.Name,
				"symbol", data.Symbol,
				"sensibleid", data.SensibleId,
			)
			// ft:utxo
			mpkey = "mp:{fu" + string(data.AddressPkh) + "}" + string(data.CodeHash) + string(data.GenesisId)
			if err := pipe.ZAdd(ctx, mpkey, &redis.Z{Score: score, Member: key}).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

		}
	}

	addrToRemove := make(map[string]bool, 1)
	tokenToRemove := make(map[string]bool, 1)
	for key, data := range utxoToRemove {
		// redis全局utxo数据清除
		pipe.Del(ctx, "u"+key)
		// redis有序utxo数据清除
		if len(data.AddressPkh) < 20 {
			// 无法识别地址，暂不记录utxo
			// if err := pipe.ZRem(ctx, "mp:utxo", key).Err(); err != nil {
			// 	panic(err)
			// }
			continue
		}

		if len(data.GenesisId) < 20 {
			// 不是合约tx，则记录address utxo
			// redis有序address utxo数据清除
			mpkey = "mp:{au" + string(data.AddressPkh) + "}"
			if err := pipe.ZRem(ctx, mpkey, key).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// balance of address
			mpkey = "mp:bl" + string(data.AddressPkh)
			if err := pipe.DecrBy(ctx, mpkey, int64(data.Satoshi)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			continue
		}

		// contract balance of address
		mpkey = "mp:cb" + string(data.AddressPkh)
		if err := pipe.DecrBy(ctx, mpkey, int64(data.Satoshi)).Err(); err != nil {
			panic(err)
		}
		if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
			panic(err)
		}

		// redis有序genesis utxo数据清除
		if data.CodeType == scriptDecoder.CodeType_NFT {
			// nft:utxo
			mpkey = "mp:{nu" + string(data.AddressPkh) + "}" + string(data.CodeHash) + string(data.GenesisId)
			if err := pipe.ZRem(ctx, mpkey, key).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// nft:utxo-detail
			mpkey = "mp:nd" + string(data.CodeHash) + string(data.GenesisId)
			if err := pipe.ZRem(ctx, mpkey, key).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// nft:owners
			mpkey = "mp:{no" + string(data.GenesisId) + string(data.CodeHash) + "}"
			if err := pipe.ZIncrBy(ctx, mpkey, -1, string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// nft:summary
			mpkey = "mp:{ns" + string(data.AddressPkh) + "}"
			if err := pipe.ZIncrBy(ctx, mpkey, -1, string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

		} else if data.CodeType == scriptDecoder.CodeType_FT {
			// ft:utxo
			mpkey = "mp:{fu" + string(data.AddressPkh) + "}" + string(data.CodeHash) + string(data.GenesisId)
			if err := pipe.ZRem(ctx, mpkey, key).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// ft:balance
			mpkey = "mp:{fb" + string(data.GenesisId) + string(data.CodeHash) + "}"
			if err := pipe.ZIncrBy(ctx, mpkey, -float64(data.Amount), string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// ft:summary
			mpkey = "mp:{fs" + string(data.AddressPkh) + "}"
			if err := pipe.ZIncrBy(ctx, mpkey, -float64(data.Amount), string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

		} else if data.CodeType == scriptDecoder.CodeType_UNIQUE {
			// ft:utxo
			mpkey = "mp:{fu" + string(data.AddressPkh) + "}" + string(data.CodeHash) + string(data.GenesisId)
			if err := pipe.ZRem(ctx, mpkey, key).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

		}

		// 记录key以备删除
		tokenToRemove[string(data.CodeHash)+string(data.GenesisId)] = true
		addrToRemove[string(data.AddressPkh)] = true
	}

	for key, data := range utxoToSpend {
		// redis全局utxo数据不能在这里清除，留给区块确认时去做
		// pipe.Del(ctx, "u"+key)

		// redis有序utxo数据添加
		score := float64(data.BlockHeight)*1000000000 + float64(data.TxIdx)
		if len(data.AddressPkh) < 20 {
			// 无法识别地址，暂不记录utxo
			// if err := pipe.ZAdd(ctx, "mp:s:utxo", &redis.Z{Score: score, Member: key}).Err(); err != nil {
			// 	panic(err)
			// }
			continue
		}

		if len(data.GenesisId) < 20 {
			// 不是合约tx，则记录address utxo
			// redis有序address utxo数据添加
			mpkey = "mp:s:{au" + string(data.AddressPkh) + "}"
			if err := pipe.ZAdd(ctx, mpkey, &redis.Z{Score: score, Member: key}).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// balance of address
			mpkey = "mp:bl" + string(data.AddressPkh)
			if err := pipe.DecrBy(ctx, mpkey, int64(data.Satoshi)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			continue
		}

		// contract balance of address
		mpkey = "mp:cb" + string(data.AddressPkh)
		if err := pipe.DecrBy(ctx, mpkey, int64(data.Satoshi)).Err(); err != nil {
			panic(err)
		}
		if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
			panic(err)
		}

		// redis有序genesis utxo数据添加
		if data.CodeType == scriptDecoder.CodeType_NFT {
			nftId := float64(data.TokenIndex)
			// nft:utxo
			mpkey = "mp:s:{nu" + string(data.AddressPkh) + "}" + string(data.CodeHash) + string(data.GenesisId)
			if err := pipe.ZAdd(ctx, mpkey,
				&redis.Z{Score: nftId, Member: key}).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// nft:utxo-detail
			mpkey = "mp:s:nd" + string(data.CodeHash) + string(data.GenesisId)
			if err := pipe.ZAdd(ctx, mpkey,
				&redis.Z{Score: nftId, Member: key}).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// nft:owners
			mpkey = "mp:{no" + string(data.GenesisId) + string(data.CodeHash) + "}"
			if err := pipe.ZIncrBy(ctx, mpkey, -1, string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// nft:summary
			mpkey = "mp:{ns" + string(data.AddressPkh) + "}"
			if err := pipe.ZIncrBy(ctx, mpkey, -1, string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

		} else if data.CodeType == scriptDecoder.CodeType_FT {
			// ft:utxo
			mpkey = "mp:s:{fu" + string(data.AddressPkh) + "}" + string(data.CodeHash) + string(data.GenesisId)
			if err := pipe.ZAdd(ctx, mpkey, &redis.Z{Score: score, Member: key}).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// ft:balance
			mpkey = "mp:{fb" + string(data.GenesisId) + string(data.CodeHash) + "}"
			if err := pipe.ZIncrBy(ctx, mpkey, -float64(data.Amount), string(data.AddressPkh)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

			// ft:summary
			mpkey = "mp:{fs" + string(data.AddressPkh) + "}"
			if err := pipe.ZIncrBy(ctx, mpkey, -float64(data.Amount), string(data.CodeHash)+string(data.GenesisId)).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

		} else if data.CodeType == scriptDecoder.CodeType_UNIQUE {
			// ft:utxo
			mpkey = "mp:s:{fu" + string(data.AddressPkh) + "}" + string(data.CodeHash) + string(data.GenesisId)
			if err := pipe.ZAdd(ctx, mpkey, &redis.Z{Score: score, Member: key}).Err(); err != nil {
				panic(err)
			}
			if err := pipe.SAdd(ctx, "mp:keys", mpkey).Err(); err != nil {
				panic(err)
			}

		}

		// 记录key以备删除
		tokenToRemove[string(data.CodeHash)+string(data.GenesisId)] = true
		addrToRemove[string(data.AddressPkh)] = true
	}

	// 删除summary 为0的记录
	for codeKey := range tokenToRemove {
		if err := pipe.ZRemRangeByScore(ctx, "mp:{no"+codeKey+"}", "0", "0").Err(); err != nil {
			panic(err)
		}
		if err := pipe.ZRemRangeByScore(ctx, "mp:{fb"+codeKey+"}", "0", "0").Err(); err != nil {
			panic(err)
		}
	}
	// 删除balance 为0的记录
	for addr := range addrToRemove {
		if err := pipe.ZRemRangeByScore(ctx, "mp:{ns"+addr+"}", "0", "0").Err(); err != nil {
			panic(err)
		}
		if err := pipe.ZRemRangeByScore(ctx, "mp:{fs"+addr+"}", "0", "0").Err(); err != nil {
			panic(err)
		}
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		panic(err)
	}
	return nil
}
