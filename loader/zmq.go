package loader

import (
	"encoding/binary"
	"log"

	"github.com/zeromq/goczmq"
)

func ZmqNotify(endpoint string, rawtx, block chan []byte) {
	subscriber, err := goczmq.NewSub(endpoint, "rawtx,hashblock")
	defer subscriber.Destroy()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("ZeroMQ started to listen for blocks")

	for {
		msg, n, err := subscriber.RecvFrame()
		if err != nil {
			log.Printf("Error ZMQ RecFrame: %s", err)
		}

		if len(msg) == 32 {
			// hashblock
			block <- msg

		} else if len(msg) == 4 {
			// id
			log.Printf("id: %d, %d", n, binary.LittleEndian.Uint32(msg))

		} else if len(msg) == 5 || len(msg) == 6 || len(msg) == 9 {
			// topic
			log.Printf("sub received: %d, %s", n, string(msg))

		} else {
			// rawtx
			rawtx <- msg
			log.Printf("tx received: %d", len(msg))
		}

		// log.Printf("received '%s'", hex.EncodeToString(msg))
	}
}
