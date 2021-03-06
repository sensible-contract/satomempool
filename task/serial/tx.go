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
	useCluster          bool
	rdb                 redis.UniversalClient
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

	addrs := viper.GetStringSlice("addrs")
	password := viper.GetString("password")
	database := viper.GetInt("database")
	dialTimeout := viper.GetDuration("dialTimeout")
	readTimeout := viper.GetDuration("readTimeout")
	writeTimeout := viper.GetDuration("writeTimeout")
	poolSize := viper.GetInt("poolSize")
	rdb = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:        addrs,
		Password:     password,
		DB:           database,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		PoolSize:     poolSize,
	})

	if len(addrs) > 1 {
		useCluster = true
	}

	SubcribeBlockSynced = rdb.Subscribe(ctx, "channel_block_sync")
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

	pipe := rdb.Pipeline()
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
		m[key] = pipe.Get(ctx, "u"+key)
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
	keys, err := rdb.SMembers(ctx, "mp:keys").Result()
	if err != nil {
		logger.Log.Info("FlushdbInRedis redis failed", zap.Error(err))
		return
	}
	logger.Log.Info("FlushdbInRedis", zap.Int("nKeys", len(keys)))

	if len(keys) == 0 {
		return
	}
	pipe := rdb.Pipeline()
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
	mpkeys := make([]string, 5*(len(utxoToRestore)+len(utxoToRemove)+len(utxoToSpend)))
	pipe := rdb.Pipeline()
	for outpointKey, data := range utxoToRestore {
		buf := make([]byte, 20+len(data.Script))
		data.Marshal(buf)
		// redis全局utxo数据添加
		// fixme: 会覆盖satoblock？
		pipe.SetNX(ctx, "u"+outpointKey, buf, 0)

		strAddressPkh := string(data.AddressPkh)
		strCodeHash := string(data.CodeHash)
		strGenesisId := string(data.GenesisId)

		// redis有序utxo数据添加
		member := &redis.Z{Score: float64(data.BlockHeight)*1000000000 + float64(data.TxIdx), Member: outpointKey}
		if len(data.AddressPkh) < 20 {
			// 无法识别地址，暂不记录utxo
			logger.Log.Info("ignore mp:utxo", zap.String("key", hex.EncodeToString([]byte(outpointKey))), zap.Float64("score", member.Score))
			// logger.Log.Info("ZAdd mp:utxo", zap.String("key", hex.EncodeToString([]byte(outpointKey))), zap.Float64("score", member.Score))
			// pipe.ZAdd(ctx, "mp:utxo", member)
			continue
		}

		if len(data.GenesisId) < 20 {
			// 不是合约tx，则记录address utxo
			// redis有序address utxo数据添加
			logger.Log.Info("ZAdd mp:au",
				zap.String("addrHex", hex.EncodeToString(data.AddressPkh)),
				zap.String("key", hex.EncodeToString([]byte(outpointKey))),
				zap.Float64("score", member.Score))
			mpkeyAU := "mp:{au" + strAddressPkh + "}"
			pipe.ZAdd(ctx, mpkeyAU, member)

			// balance of address
			logger.Log.Info("IncrBy mp:bl",
				zap.String("addrHex", hex.EncodeToString(data.AddressPkh)),
				zap.Uint64("satoshi", data.Satoshi))
			mpkeyBL := "mp:bl" + strAddressPkh
			pipe.IncrBy(ctx, mpkeyBL, int64(data.Satoshi))

			mpkeys = append(mpkeys, mpkeyAU, mpkeyBL)
			continue
		}

		// contract balance of address
		logger.Log.Info("IncrBy mp:cb",
			zap.String("addrHex", hex.EncodeToString(data.AddressPkh)),
			zap.Uint64("satoshi", data.Satoshi))
		mpkeyCB := "mp:cb" + strAddressPkh
		pipe.IncrBy(ctx, mpkeyCB, int64(data.Satoshi))
		mpkeys = append(mpkeys, mpkeyCB)

		// redis有序genesis utxo数据添加
		if data.CodeType == scriptDecoder.CodeType_NFT {
			logger.Log.Info("=== update nft")

			pipe.HSet(ctx, "ni"+strCodeHash+strGenesisId,
				"metatxid", data.MetaTxId,
				"metavout", data.MetaOutputIndex,
				"supply", data.TokenSupply,
				"sensibleid", data.SensibleId,
			)

			member.Score = float64(data.TokenIndex)
			mpkeyNU := "mp:{nu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			pipe.ZAdd(ctx, mpkeyNU, member) // nft:utxo
			mpkeyND := "mp:nd" + strCodeHash + strGenesisId
			pipe.ZAdd(ctx, mpkeyND, member) // nft:utxo-detail
			mpkeyNO := "mp:{no" + strGenesisId + strCodeHash + "}"
			pipe.ZIncrBy(ctx, mpkeyNO, 1, strAddressPkh) // nft:owners
			mpkeyNS := "mp:{ns" + strAddressPkh + "}"
			pipe.ZIncrBy(ctx, mpkeyNS, 1, strCodeHash+strGenesisId) // nft:summary

			mpkeys = append(mpkeys, mpkeyNU, mpkeyND, mpkeyNO, mpkeyNS)
		} else if data.CodeType == scriptDecoder.CodeType_FT {
			// ft:info
			logger.Log.Info("=== update ft")
			pipe.HSet(ctx, "fi"+strCodeHash+strGenesisId,
				"decimal", data.Decimal,
				"name", data.Name,
				"symbol", data.Symbol,
				"sensibleid", data.SensibleId,
			)

			mpkeyFU := "mp:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			pipe.ZAdd(ctx, mpkeyFU, member) // ft:utxo
			mpkeyFB := "mp:{fb" + strGenesisId + strCodeHash + "}"
			pipe.ZIncrBy(ctx, mpkeyFB, float64(data.Amount), strAddressPkh) // ft:balance
			mpkeyFS := "mp:{fs" + strAddressPkh + "}"
			pipe.ZIncrBy(ctx, mpkeyFS, float64(data.Amount), strCodeHash+strGenesisId) // ft:summary

			mpkeys = append(mpkeys, mpkeyFU, mpkeyFB, mpkeyFS)
		} else if data.CodeType == scriptDecoder.CodeType_UNIQUE {
			// ft:info
			logger.Log.Info("=== update unique")
			pipe.HSet(ctx, "fi"+strCodeHash+strGenesisId,
				"decimal", data.Decimal,
				"name", data.Name,
				"symbol", data.Symbol,
				"sensibleid", data.SensibleId,
			)

			mpkeyFU := "mp:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			pipe.ZAdd(ctx, mpkeyFU, member) // ft:utxo

			mpkeys = append(mpkeys, mpkeyFU)
		}
	}

	addrToRemove := make(map[string]bool, 1)
	tokenToRemove := make(map[string]bool, 1)
	for key, data := range utxoToRemove {
		// redis全局utxo数据清除
		// 暂时不清除
		// pipe.Del(ctx, "u"+key)

		strAddressPkh := string(data.AddressPkh)
		strCodeHash := string(data.CodeHash)
		strGenesisId := string(data.GenesisId)

		// redis有序utxo数据清除
		if len(data.AddressPkh) < 20 {
			// 无法识别地址，暂不记录utxo
			// pipe.ZRem(ctx, "mp:utxo", key)
			continue
		}

		if len(data.GenesisId) < 20 {
			// 不是合约tx，则记录address utxo
			// redis有序address utxo数据清除
			mpkeyAU := "mp:{au" + strAddressPkh + "}"
			pipe.ZRem(ctx, mpkeyAU, key)

			// balance of address
			mpkeyBL := "mp:bl" + strAddressPkh
			pipe.DecrBy(ctx, mpkeyBL, int64(data.Satoshi))
			continue
		}

		// contract balance of address
		mpkeyCB := "mp:cb" + strAddressPkh
		pipe.DecrBy(ctx, mpkeyCB, int64(data.Satoshi))

		// redis有序genesis utxo数据清除
		if data.CodeType == scriptDecoder.CodeType_NFT {
			mpkeyNU := "mp:{nu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			pipe.ZRem(ctx, mpkeyNU, key) // nft:utxo
			mpkeyND := "mp:nd" + strCodeHash + strGenesisId
			pipe.ZRem(ctx, mpkeyND, key) // nft:utxo-detail
			mpkeyNO := "mp:{no" + strGenesisId + strCodeHash + "}"
			pipe.ZIncrBy(ctx, mpkeyNO, -1, strAddressPkh) // nft:owners
			mpkeyNS := "mp:{ns" + strAddressPkh + "}"
			pipe.ZIncrBy(ctx, mpkeyNS, -1, strCodeHash+strGenesisId) // nft:summary

		} else if data.CodeType == scriptDecoder.CodeType_FT {
			mpkeyFU := "mp:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			pipe.ZRem(ctx, mpkeyFU, key) // ft:utxo
			mpkeyFB := "mp:{fb" + strGenesisId + strCodeHash + "}"
			pipe.ZIncrBy(ctx, mpkeyFB, -float64(data.Amount), strAddressPkh) // ft:balance
			mpkeyFS := "mp:{fs" + strAddressPkh + "}"
			pipe.ZIncrBy(ctx, mpkeyFS, -float64(data.Amount), strCodeHash+strGenesisId) // ft:summary

		} else if data.CodeType == scriptDecoder.CodeType_UNIQUE {
			mpkeyFU := "mp:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			pipe.ZRem(ctx, mpkeyFU, key) // ft:utxo
		}

		// 记录key以备删除
		tokenToRemove[strGenesisId+strCodeHash] = true
		addrToRemove[strAddressPkh] = true
	}

	for outpointKey, data := range utxoToSpend {
		// redis全局utxo数据不能在这里清除，留给区块确认时去做
		// pipe.Del(ctx, "u"+key)

		strAddressPkh := string(data.AddressPkh)
		strCodeHash := string(data.CodeHash)
		strGenesisId := string(data.GenesisId)

		// redis有序utxo数据添加
		member := &redis.Z{Score: float64(data.BlockHeight)*1000000000 + float64(data.TxIdx), Member: outpointKey}
		if len(data.AddressPkh) < 20 {
			// 无法识别地址，暂不记录utxo
			// pipe.ZAdd(ctx, "mp:s:utxo", member)
			continue
		}

		if len(data.GenesisId) < 20 {
			// 不是合约tx，则记录address utxo
			// redis有序address utxo数据添加
			mpkeyAU := "mp:s:{au" + strAddressPkh + "}"
			pipe.ZAdd(ctx, mpkeyAU, member)

			// balance of address
			mpkeyBL := "mp:bl" + strAddressPkh
			pipe.DecrBy(ctx, mpkeyBL, int64(data.Satoshi))

			mpkeys = append(mpkeys, mpkeyAU, mpkeyBL)
			continue
		}

		// contract balance of address
		mpkeyCB := "mp:cb" + strAddressPkh
		pipe.DecrBy(ctx, mpkeyCB, int64(data.Satoshi))
		mpkeys = append(mpkeys, mpkeyCB)

		// redis有序genesis utxo数据添加
		if data.CodeType == scriptDecoder.CodeType_NFT {
			member.Score = float64(data.TokenIndex)
			mpkeyNU := "mp:s:{nu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			pipe.ZAdd(ctx, mpkeyNU, member) // nft:utxo
			mpkeyND := "mp:s:nd" + strCodeHash + strGenesisId
			pipe.ZAdd(ctx, mpkeyND, member) // nft:utxo-detail
			mpkeyNO := "mp:{no" + strGenesisId + strCodeHash + "}"
			pipe.ZIncrBy(ctx, mpkeyNO, -1, strAddressPkh) // nft:owners
			mpkeyNS := "mp:{ns" + strAddressPkh + "}"
			pipe.ZIncrBy(ctx, mpkeyNS, -1, strCodeHash+strGenesisId) // nft:summary

			mpkeys = append(mpkeys, mpkeyNU, mpkeyND, mpkeyNO, mpkeyNS)
		} else if data.CodeType == scriptDecoder.CodeType_FT {
			mpkeyFU := "mp:s:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			pipe.ZAdd(ctx, mpkeyFU, member) // ft:utxo
			mpkeyFB := "mp:{fb" + strGenesisId + strCodeHash + "}"
			pipe.ZIncrBy(ctx, mpkeyFB, -float64(data.Amount), strAddressPkh) // ft:balance
			mpkeyFS := "mp:{fs" + strAddressPkh + "}"
			pipe.ZIncrBy(ctx, mpkeyFS, -float64(data.Amount), strCodeHash+strGenesisId) // ft:summary

			mpkeys = append(mpkeys, mpkeyFU, mpkeyFB, mpkeyFS)
		} else if data.CodeType == scriptDecoder.CodeType_UNIQUE {
			mpkeyFU := "mp:s:{fu" + strAddressPkh + "}" + strCodeHash + strGenesisId
			pipe.ZAdd(ctx, mpkeyFU, member) // ft:utxo

			mpkeys = append(mpkeys, mpkeyFU)
		}

		// 记录key以备删除
		tokenToRemove[strGenesisId+strCodeHash] = true
		addrToRemove[strAddressPkh] = true
	}

	// 删除summary 为0的记录
	for codeKey := range tokenToRemove {
		pipe.ZRemRangeByScore(ctx, "mp:{no"+codeKey+"}", "0", "0")
		pipe.ZRemRangeByScore(ctx, "mp:{fb"+codeKey+"}", "0", "0")
	}
	// 删除balance 为0的记录
	for addr := range addrToRemove {
		pipe.ZRemRangeByScore(ctx, "mp:{ns"+addr+"}", "0", "0")
		pipe.ZRemRangeByScore(ctx, "mp:{fs"+addr+"}", "0", "0")
	}

	// 记录所有的mp:keys，以备区块确认后直接删除重来
	for _, mpkey := range mpkeys {
		pipe.SAdd(ctx, "mp:keys", mpkey)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		panic(err)
	}
	return nil
}
