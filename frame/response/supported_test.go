package response

import (
	"testing"

	"github.com/mmatczuk/scylla-go-driver/frame"

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
				t.Fatal("failure buffer not empty after read")
			}
		})
	}
}

func TestParseScyllaSupported(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		content  Supported
		expected ScyllaSupported
	}{
		{
			name: "Smoke test",
			content: Supported{frame.StringMultiMap{
				ScyllaNrShards:          []string{"52213"},
				ScyllaShardingIgnoreMSB: []string{"22"},
				ScyllaPartitioner:       []string{"org.apache.cassandra.dht.Murmur3Partitioner"},
				ScyllaShardingAlgorithm: []string{"biased-token-round-robin"},
			}},
			expected: ScyllaSupported{
				NrShards:          52213,
				MsbIgnore:         22,
				Partitioner:       "org.apache.cassandra.dht.Murmur3Partitioner",
				ShardingAlgorithm: "biased-token-round-robin",
			},
		},
		{
			name: "All options",
			content: Supported{frame.StringMultiMap{
				ScyllaShard:             []string{"3"},
				ScyllaNrShards:          []string{"12"},
				ScyllaShardingIgnoreMSB: []string{"22"},
				ScyllaPartitioner:       []string{"org.apache.cassandra.dht.Murmur3Partitioner"},
				ScyllaShardingAlgorithm: []string{"biased-token-round-robin"},
				ScyllaShardAwarePort:    []string{"19042"},
				ScyllaShardAwarePortSSL: []string{"19142"},
			}},
			expected: ScyllaSupported{
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
			out := tc.content.ScyllaSupported()
			if diff := cmp.Diff(*out, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
