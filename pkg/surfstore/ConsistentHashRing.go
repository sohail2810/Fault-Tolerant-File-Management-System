package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	keys := make([]string, 0, len(c.ServerMap))
	for k := range c.ServerMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) < 2 {
		for _, s := range c.ServerMap {
			return s
		}
	}
	for _, k := range keys {
		if k > blockId {
			return c.ServerMap[k]
		}
	}
	return c.ServerMap[keys[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	m := make(map[string]string)
	for _, addr := range serverAddrs {
		hash_value := GetBlockHashString([]byte("blockstore" + addr))
		m[hash_value] = addr
	}
	chr := ConsistentHashRing{ServerMap: m}
	return &chr
}

func Hash(addr string) {
	panic("unimplemented")
}
