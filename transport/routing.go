package transport

import (
	"math/rand"

	"github.com/mmatczuk/scylla-go-driver/transport/murmur"
)

const (
	// Range of ports that can be used to establish connection.
	maxPort = 65535
	minPort = 49152
)

type ShardInfo struct {
	Shard     uint16
	NrShards  uint16
	MsbIgnore uint8
}

// RandomShardPort returns randomly generated port that can be used
// to establish connection to a specific shard on scylla node.
func RandomShardPort(si ShardInfo) uint16 {
	maxRange := int(maxPort - si.NrShards + 1)
	minRange := int(minPort + si.NrShards - 1)
	r := uint16(rand.Intn(maxRange-minRange+1) + minRange)
	return r/si.NrShards*si.NrShards + si.Shard
}

// ShardPortIterator returns iterator for consecutive ports that are
// mapped to a specific shard on scylla node.
func ShardPortIterator(si ShardInfo) func() uint16 {
	port := RandomShardPort(si)

	return func() uint16 {
		port += si.NrShards
		if port > maxPort {
			port = (minPort+si.NrShards-1)/si.NrShards*si.NrShards + si.Shard
		}
		return port
	}
}

// Token is used to identify both nodes and partitions, it's value is hashed partition key.
type Token struct {
	value int64
}

// MurmurToken is a function which given partition key hashes it, using Murmurhash3.
func MurmurToken(partitionKey []byte) Token { // nolint:unused // This will be used.
	h := murmur.Hash3(partitionKey)
	return Token{value: h}
}
