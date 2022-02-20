package response

import (
	"testing"

	"scylla-go-driver/frame"

	"github.com/google/go-cmp/cmp"
)

func TestSupportedEncodeDecode(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  []byte
		expected Supported
	}{
		{
			name:    "Smoke test",
			content: []byte{0x00, 0x01, 0x00, 0x01, 0x61, 0x00, 0x02, 0x00, 0x01, 0x61, 0x00, 0x01, 0x62},
			expected: Supported{
				Options: frame.StringMultiMap{"a": {"a", "b"}},
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out frame.Buffer
			out.Write(tc.content)
			a := ParseSupported(&out)
			if diff := cmp.Diff(*a, tc.expected); diff != "" {
				t.Fatal(diff)
			}
			if len(out.Bytes()) != 0 {
				t.Fatal("Failure buffer not empty after read.")
			}
		})
	}
}

func TestParseScyllaSupported(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  Supported
		expected frame.ScyllaSupported
	}{
		{
			name: "Smoke test",
			content: Supported{frame.StringMultiMap{
				frame.ScyllaNrShards:          []string{"52213"},
				frame.ScyllaShardingIgnoreMSB: []string{"22"},
				frame.ScyllaPartitioner:       []string{"org.apache.cassandra.dht.Murmur3Partitioner"},
				frame.ScyllaShardingAlgorithm: []string{"biased-token-round-robin"},
			}},
			expected: frame.ScyllaSupported{
				NrShards:          52213,
				MsbIgnore:         22,
				Partitioner:       "org.apache.cassandra.dht.Murmur3Partitioner",
				ShardingAlgorithm: "biased-token-round-robin",
			},
		},
		{
			name: "All options",
			content: Supported{frame.StringMultiMap{
				frame.ScyllaShard:             []string{"3"},
				frame.ScyllaNrShards:          []string{"12"},
				frame.ScyllaShardingIgnoreMSB: []string{"22"},
				frame.ScyllaPartitioner:       []string{"org.apache.cassandra.dht.Murmur3Partitioner"},
				frame.ScyllaShardingAlgorithm: []string{"biased-token-round-robin"},
				frame.ScyllaShardAwarePort:    []string{"19042"},
				frame.ScyllaShardAwarePortSSL: []string{"19142"},
			}},
			expected: frame.ScyllaSupported{
				Shard:             3,
				NrShards:          12,
				MsbIgnore:         22,
				Partitioner:       "org.apache.cassandra.dht.Murmur3Partitioner",
				ShardingAlgorithm: "biased-token-round-robin",
				ShardAwarePort:    19042,
				ShardAwarePortSSL: 19142,
			},
		},
	}
	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := tc.content.ParseScyllaSupported()
			if diff := cmp.Diff(*out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
