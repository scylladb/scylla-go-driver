package response

import (
	"log"
	"strconv"

	"scylla-go-driver/frame"
)

// Supported spec: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L537
type Supported struct {
	Options frame.StringMultiMap
}

func ParseSupported(b *frame.Buffer) *Supported {
	return &Supported{
		Options: b.ReadStringMultiMap(),
	}
}

func (s *Supported) ParseScyllaSupported() *frame.ScyllaSupported {
	// This variable is filled during function
	var si frame.ScyllaSupported

	if s, ok := s.Options[frame.ScyllaShard]; ok {
		if shard, err := strconv.ParseUint(s[0], 10, 16); err != nil {
			if frame.Debug {
				log.Printf("scylla: failed to parse %s value %v: %s", frame.ScyllaShard, s, err)
			}
		} else {
			si.Shard = uint16(shard)
		}
	}
	if s, ok := s.Options[frame.ScyllaNrShards]; ok {
		if nrShards, err := strconv.ParseUint(s[0], 10, 16); err != nil {
			if frame.Debug {
				log.Printf("scylla: failed to parse %s value %v: %s", frame.ScyllaNrShards, s, err)
			}
		} else {
			si.NrShards = uint16(nrShards)
		}
	}
	if s, ok := s.Options[frame.ScyllaShardingIgnoreMSB]; ok {
		if msbIgnore, err := strconv.ParseUint(s[0], 10, 8); err != nil {
			if frame.Debug {
				log.Printf("scylla: failed to parse %s value %v: %s", frame.ScyllaShardingIgnoreMSB, s, err)
			}
		} else {
			si.MsbIgnore = uint8(msbIgnore)
		}
	}
	if s, ok := s.Options[frame.ScyllaShardAwarePort]; ok {
		if shardAwarePort, err := strconv.ParseUint(s[0], 10, 16); err != nil {
			if frame.Debug {
				log.Printf("scylla: failed to parse %s value %v: %s", frame.ScyllaShardAwarePort, s, err)
			}
		} else {
			si.ShardAwarePort = uint16(shardAwarePort)
		}
	}
	if s, ok := s.Options[frame.ScyllaShardAwarePortSSL]; ok {
		if shardAwarePortSSL, err := strconv.ParseUint(s[0], 10, 16); err != nil {
			if frame.Debug {
				log.Printf("scylla: failed to parse %s value %v: %s", frame.ScyllaShardAwarePortSSL, s, err)
			}
		} else {
			si.ShardAwarePortSSL = uint16(shardAwarePortSSL)
		}
	}

	if s, ok := s.Options[frame.ScyllaPartitioner]; ok {
		si.Partitioner = s[0]
	}
	if s, ok := s.Options[frame.ScyllaShardingAlgorithm]; ok {
		si.ShardingAlgorithm = s[0]
	}

	// Currently, only one sharding algorithm is defined, and it is 'biased-token-round-robin'.
	// For now we only support 'Murmur3Partitioner', it is due to change in the future.
	if si.Partitioner != "org.apache.cassandra.dht.Murmur3Partitioner" ||
		si.ShardingAlgorithm != "biased-token-round-robin" || si.NrShards == 0 || si.MsbIgnore == 0 {
		if frame.Debug {
			log.Printf(`scylla: unsupported sharding configuration, partitioner=%s, algorithm=%s, 
						no_shards=%d, msb_ignore=%d`, si.Partitioner, si.ShardingAlgorithm, si.NrShards, si.MsbIgnore)
		}
		return &frame.ScyllaSupported{}
	}

	return &si
}
