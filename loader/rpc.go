package loader

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"satomempool/logger"

	"github.com/spf13/viper"
	"github.com/ybbus/jsonrpc/v2"
	"go.uber.org/zap"
)

var rpcClient jsonrpc.RPCClient

func init() {
	viper.SetConfigFile("conf/chain.yaml")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		} else {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	}

	rpcAddress := viper.GetString("rpc")
	rpcAuth := viper.GetString("rpc_auth")
	rpcClient = jsonrpc.NewClientWithOpts(rpcAddress, &jsonrpc.RPCClientOpts{
		CustomHeaders: map[string]string{
			"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte(rpcAuth)),
		},
	})
}

func GetRawMemPoolRPC() []interface{} {
	response, err := rpcClient.Call("getrawmempool", []string{})
	if err != nil {
		logger.Log.Info("call failed", zap.Error(err))
		return nil
	}

	if response.Error != nil {
		logger.Log.Info("Receive remote return", zap.Any("response", response))
		return nil
	}

	txids, ok := response.Result.([]interface{})
	if !ok {
		logger.Log.Info("mempool not list: %T", zap.Any("response", response.Result))
		return nil
	}
	return txids
}

func GetRawTxRPC(txid interface{}) []byte {
	response, err := rpcClient.Call("getrawtransaction", []interface{}{txid})
	if err != nil {
		logger.Log.Info("call failed", zap.Error(err))
		return nil
	}

	if response.Error != nil {
		logger.Log.Info("Receive remote return", zap.Any("response", response))
		return nil
	}

	rawtxString, ok := response.Result.(string)
	if !ok {
		logger.Log.Info("mempool entry not string")
		return nil
	}

	rawtx, err := hex.DecodeString(rawtxString)
	if err != nil {
		logger.Log.Info("rawtx hex err", zap.String("rawtx[:64]", rawtxString[:64]))
		return nil
	}

	return rawtx
}
