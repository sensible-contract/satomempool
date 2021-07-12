package utils

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"satomempool/model"
)

func DecodeVarIntForBlock(raw []byte) (cnt uint, cnt_size uint) {
	if raw[0] < 0xfd {
		return uint(raw[0]), 1
	}

	if raw[0] == 0xfd {
		return uint(binary.LittleEndian.Uint16(raw[1:3])), 3
	} else if raw[0] == 0xfe {
		return uint(binary.LittleEndian.Uint32(raw[1:5])), 5
	}
	return uint(binary.LittleEndian.Uint64(raw[1:9])), 9
}

func GetHash256(data []byte) (hash []byte) {
	sha := sha256.New()
	sha.Write(data[:])
	tmp := sha.Sum(nil)
	sha.Reset()
	sha.Write(tmp)
	hash = sha.Sum(nil)
	return
}

func HashString(data []byte) (res string) {
	length := 32
	reverseData := make([]byte, length)

	// need reverse
	for i := 0; i < length; i++ {
		reverseData[i] = data[length-i-1]
	}

	return hex.EncodeToString(reverseData)
}

func isTxFinal(tx *model.Tx) bool {
	if tx.LockTime == 0 {
		return true
	}

	isFinal := true
	for _, input := range tx.TxIns {
		if input.Sequence != math.MaxUint32 {
			isFinal = false
		}
	}
	return isFinal
}

func IsTxNonFinal(tx *model.Tx, nonFinalTxs map[string]bool) bool {
	if !isTxFinal(tx) {
		return true
	}

	for _, input := range tx.TxIns {
		if _, ok := nonFinalTxs[input.InputHashHex]; ok {
			return true
		}
	}
	return false
}
