package loader

import (
	"satomempool/logger"

	"github.com/zeromq/goczmq"
	"go.uber.org/zap"
)

func ZmqNotify(endpoint string, rawtx chan []byte) {
	logger.Log.Info("ZeroMQ started to listen for txs")
	subscriber, err := goczmq.NewSub(endpoint, "rawtx")
	if err != nil {
		logger.Log.Fatal("ZMQ connect failed", zap.Error(err))
		return
	}
	defer subscriber.Destroy()

	for {
		msg, _, err := subscriber.RecvFrame()
		if err != nil {
			logger.Log.Info("Error ZMQ RecFrame: %s", zap.Error(err))
		}

		if len(msg) == 4 {
			// id
			// logger.Log.Info("id: %d, %d", zap.Int("n", n), zap.Int("id", binary.LittleEndian.Uint32(msg)))

		} else if len(msg) == 5 || len(msg) == 6 || len(msg) == 9 {
			// topic
			// logger.Log.Info("sub received", zap.Int("n", n), zap.String("topic", string(msg)))

		} else {
			// rawtx
			rawtx <- msg
			// logger.Log.Info("tx received", zap.Int("n", n), zap.Int("rawtxLen", len(msg)))
		}
	}
}
