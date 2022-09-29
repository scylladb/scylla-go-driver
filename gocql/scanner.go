package gocql

import (
	"errors"
	"fmt"

	"github.com/scylladb/scylla-go-driver/frame"
)

type Scanner interface {
	// Next advances the row pointer to point at the next row, the row is valid until
	// the next call of Next. It returns true if there is a row which is available to be
	// scanned into with Scan.
	// Next must be called before every call to Scan.
	Next() bool

	// Scan copies the current row's columns into dest. If the length of dest does not equal
	// the number of columns returned in the row an error is returned. If an error is encountered
	// when unmarshalling a column into the value in dest an error is returned and the row is invalidated
	// until the next call to Next.
	// Next must be called before calling Scan, if it is not an error is returned.
	Scan(...interface{}) error

	// Err returns the if there was one during iteration that resulted in iteration being unable to complete.
	// Err will also release resources held by the iterator, the Scanner should not used after being called.
	Err() error
}

type iterScanner struct {
	iter   *Iter
	result frame.Row
	valid  bool
}

func (is *iterScanner) Next() bool {
	iter := is.iter
	if iter.err != nil {
		return false
	}

	var err error
	is.result, err = iter.it.Next()
	is.valid = err != nil
	return is.valid
}

func scanColumn(p []byte, col ColumnInfo, dest []interface{}) (int, error) {
	if dest[0] == nil {
		return 1, nil
	}

	if col.TypeInfo.Type() == TypeTuple {
		// this will panic, actually a bug, please report
		tuple := col.TypeInfo.(TupleTypeInfo)

		count := len(tuple.Elems)
		// here we pass in a slice of the struct which has the number number of
		// values as elements in the tuple
		if err := Unmarshal(col.TypeInfo, p, dest[:count]); err != nil {
			return 0, err
		}
		return count, nil
	} else {
		if err := Unmarshal(col.TypeInfo, p, dest[0]); err != nil {
			return 0, err
		}
		return 1, nil
	}
}

func (is *iterScanner) Scan(dest ...interface{}) error {
	if !is.valid {
		return errors.New("gocql: Scan called without calling Next")
	}

	iter := is.iter
	// currently only support scanning into an expand tuple, such that its the same
	// as scanning in more values from a single column
	if len(dest) != len(iter.Columns()) {
		return fmt.Errorf("gocql: not enough columns to scan into: have %d want %d", len(dest), len(iter.Columns()))
	}

	// i is the current position in dest, could posible replace it and just use
	// slices of dest
	i := 0
	var err error
	for _, col := range iter.Columns() {
		var n int
		n, err = scanColumn(is.result[i].Value, col, dest[i:])
		if err != nil {
			break
		}
		i += n
	}

	is.valid = false
	return err
}

func (is *iterScanner) Err() error {
	iter := is.iter
	is.iter = nil
	is.result = nil
	is.valid = false
	return iter.Close()
}

// Scanner returns a row Scanner which provides an interface to scan rows in a manner which is
// similar to database/sql. The iter should NOT be used again after calling this method.
func (iter *Iter) Scanner() Scanner {
	if iter == nil {
		return nil
	}

	return &iterScanner{iter: iter, result: make(frame.Row, len(iter.Columns()))}
}
