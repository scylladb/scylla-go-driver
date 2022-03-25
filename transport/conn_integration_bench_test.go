//go:build integration

package transport

import (
	"sync"
	"testing"

	"scylla-go-driver/frame"
)

var benchmarkConnQueryResult QueryResult

func BenchmarkConnQueryIntegration(b *testing.B) {
	h := newConnTestHelper(b)
	h.applyFixture()
	defer h.close()

	query := Statement{Content: "SELECT * FROM mykeyspace.users", Consistency: frame.ONE}

	var (
		r   QueryResult
		err error
	)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		r, err = h.conn.Query(query, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
	benchmarkConnQueryResult = r
}

func BenchmarkConnAsyncQueryIntegration(b *testing.B) {
	h := newConnTestHelper(b)
	h.applyFixture()
	defer h.close()

	query := Statement{Content: "SELECT * FROM mykeyspace.users", Consistency: frame.ONE}

	var (
		wg sync.WaitGroup
		fn = func(fr QueryResult, err error) {
			wg.Done()
		}
	)

	b.ResetTimer()
	wg.Add(b.N)
	for n := 0; n < b.N; n++ {
		h.conn.QueryAsync(query, nil, fn)
	}
	wg.Wait()
}
