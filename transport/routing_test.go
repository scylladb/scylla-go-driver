package transport

import "testing"

func TestShardPortIterator(t *testing.T) {
	t.Parallel()
	for shards := 1; shards < 128; shards++ {
		for shard := 0; shard < shards; shard++ {
			si := ShardInfo{
				Shard:    uint16(shard),
				NrShards: uint16(shards),
			}

			it := ShardPortIterator(si)
			finishPort := it()
			for port := it(); port != finishPort; port = it() {
				if port < minPort || port > maxPort {
					t.Fatalf("port %d is not in range [%d, %d]", port, minPort, maxPort)
				}

				if int(port)%shards != shard {
					t.Fatalf("port %d doesn't correspond to shard %d", port, shard)
				}
			}
		}
	}
}
