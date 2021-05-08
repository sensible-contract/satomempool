package loader

import (
	"encoding/base64"
	"encoding/hex"
	"log"

	"github.com/ybbus/jsonrpc/v2"
)

var rpcClient jsonrpc.RPCClient

func init() {
	rpcAddress := "http://localhost:16332"
	rpcClient = jsonrpc.NewClientWithOpts(rpcAddress, &jsonrpc.RPCClientOpts{
		CustomHeaders: map[string]string{
			"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte("jie"+":"+"jIang_jIe1234567")),
		},
	})
}

func GetRawMemPoolRPC() []interface{} {
	response, err := rpcClient.Call("getrawmempool", []string{})
	if err != nil {
		log.Println("call failed:", err)
		return nil
	}

	if response.Error != nil {
		log.Println("Receive remote return:", response)
		return nil
	}

	txids, ok := response.Result.([]interface{})
	if !ok {
		log.Printf("mempool not list: %T", response.Result)
		return nil
	}
	return txids
}

func GetRawTxRPC(txid interface{}) []byte {
	response, err := rpcClient.Call("getrawtransaction", []interface{}{txid})
	if err != nil {
		log.Println("call failed:", err)
		return nil
	}

	if response.Error != nil {
		log.Println("Receive remote return:", response)
		return nil
	}

	rawtxString, ok := response.Result.(string)
	if !ok {
		log.Println("mempool entry not string")
		return nil
	}

	rawtx, err := hex.DecodeString(rawtxString)
	if err != nil {
		log.Println("rawtx hex err:", rawtxString[:64])
		return nil
	}

	return rawtx
}
