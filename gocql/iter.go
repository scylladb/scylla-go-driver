package gocql

import (
	"fmt"
	"reflect"

	"github.com/scylladb/scylla-go-driver"
	"github.com/scylladb/scylla-go-driver/frame"
)

type iterr interface {
	Columns() []frame.ColumnSpec
	NumRows() int
	Close() error
	Next() (frame.Row, error)
	PageState() []byte
}

type Iter struct {
	it  iterr
	err error
	row frame.Row
	// First Next call should be done on iter creation, rest on Scan
	dontSkipNext bool
}

func newIter(it iterr) *Iter {
	ret := &Iter{it: it}
	ret.row, ret.err = it.Next()
	ret.err = dropNoMoreRows(ret.err)
	return ret
}

func (it *Iter) Columns() []ColumnInfo {
	if it.err != nil {
		return nil
	}

	c := it.it.Columns()
	cols := make([]ColumnInfo, len(c))
	for i, v := range c {
		typ := WrapOption(&v.Type)
		cols[i] = ColumnInfo{
			Keyspace: v.Keyspace,
			Table:    v.Table,
			Name:     v.Name,
			TypeInfo: typ,
		}
	}

	return cols
}

func (it *Iter) NumRows() int {
	return it.it.NumRows()
}

func dropNoMoreRows(err error) error {
	if err != scylla.ErrNoMoreRows {
		return err
	}
	return nil
}

func (it *Iter) Close() error {
	if it.err != nil {
		return dropNoMoreRows(it.err)
	}

	return dropNoMoreRows(it.it.Close())
}

func (it *Iter) Scan(dest ...interface{}) bool {
	if it.err != nil {
		return false
	}

	if it.dontSkipNext {
		it.row, it.err = it.it.Next()
		it.err = dropNoMoreRows(it.err)
	} else {
		it.dontSkipNext = true
	}
	if it.err != nil {
		return false
	}

	if len(it.row) == 0 {
		return false
	}

	if len(dest) != len(it.row) {
		it.err = fmt.Errorf("expected %d columns, got %d", len(dest), len(it.row))
		return false
	}

	for i := range dest {
		if dest[i] == nil {
			continue
		}
		it.err = Unmarshal(WrapOption(it.row[i].Type), it.row[i].Value, dest[i])
		if it.err != nil {
			return false
		}
	}

	return true
}

func (it *Iter) PageState() []byte {
	return it.it.PageState()
}

type RowData struct {
	Columns []string
	Values  []interface{}
}

// TupeColumnName will return the column name of a tuple value in a column named
// c at index n. It should be used if a specific element within a tuple is needed
// to be extracted from a map returned from SliceMap or MapScan.
func TupleColumnName(c string, n int) string {
	return fmt.Sprintf("%s[%d]", c, n)
}

func (iter *Iter) RowData() (RowData, error) {
	if iter.err != nil {
		return RowData{}, iter.err
	}

	columns := make([]string, 0, len(iter.Columns()))
	values := make([]interface{}, 0, len(iter.Columns()))

	for _, column := range iter.Columns() {
		if c, ok := column.TypeInfo.(TupleTypeInfo); !ok {
			val := column.TypeInfo.New()
			columns = append(columns, column.Name)
			values = append(values, val)
		} else {
			for i, elem := range c.Elems {
				columns = append(columns, TupleColumnName(column.Name, i))
				values = append(values, elem.New())
			}
		}
	}

	return RowData{
		Columns: columns,
		Values:  values,
	}, nil
}

func (it *Iter) MapScan(m map[string]interface{}) bool {
	if it.err != nil {
		return false
	}

	// Not checking for the error because we just did
	rowData, _ := it.RowData()

	for i, col := range rowData.Columns {
		if dest, ok := m[col]; ok {
			rowData.Values[i] = dest
		}
	}
	if it.Scan(rowData.Values...) {
		rowData.rowMap(m)
		return true
	}
	return false
}

func dereference(i interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(i)).Interface()
}
func (r *RowData) rowMap(m map[string]interface{}) {
	for i, column := range r.Columns {
		val := dereference(r.Values[i])
		if valVal := reflect.ValueOf(val); valVal.Kind() == reflect.Slice {
			valCopy := reflect.MakeSlice(valVal.Type(), valVal.Len(), valVal.Cap())
			reflect.Copy(valCopy, valVal)
			m[column] = valCopy.Interface()
		} else {
			m[column] = val
		}
	}
}
