package response

//import (
//	"bytes"
//
//	"scylla-go-driver/frame"
//)
//
//// FIXME: Needs more thinking about what goes here.
//type ColumnType interface{}
//type CQLValue = interface{}
//
//func readColumnType(b *bytes.Buffer) ColumnType {
//	return "FIXME"
//}
//
//func readCQLValue(b *bytes.Buffer, ctype ColumnType) CQLValue {
//	return "FIXME"
//}
//
//type ColumnInfo struct {
//	keyspace string
//	table    string
//	name     string
//
//	columnType ColumnType
//}
//
//type resultMetadata struct {
//	flags frame.Int
//
//	// nil if flagPagingState is not set
//	pagingState frame.Bytes
//
//	colCnt frame.Int
//	cols   []ColumnInfo
//}
//
//func readColsSpec(b *bytes.Buffer, colCnt frame.Int, keyspace, table string) []ColumnInfo {
//	cols := make([]ColumnInfo, colCnt)
//	for i := range cols {
//		cols[i] = ColumnInfo{
//			keyspace:   keyspace,
//			table:      table,
//			name:       frame.ReadString(b),
//			columnType: readColumnType(b),
//		}
//	}
//
//	return cols
//}
//
//func readColsUnspec(b *bytes.Buffer, colCnt frame.Int) []ColumnInfo {
//	cols := make([]ColumnInfo, colCnt)
//	for i := range cols {
//		cols[i] = ColumnInfo{
//			keyspace:   frame.ReadString(b),
//			table:      frame.ReadString(b),
//			name:       frame.ReadString(b),
//			columnType: readColumnType(b),
//		}
//	}
//
//	return cols
//}
//
//func readResultMetadata(b *bytes.Buffer) resultMetadata {
//	ret := resultMetadata{
//		flags:  frame.ReadInt(b),
//		colCnt: frame.ReadInt(b),
//	}
//
//	if ret.flags&flagHasMorePages == flagHasMorePages {
//		ret.pagingState = frame.ReadBytes(b)
//	}
//
//	if ret.flags&flagNoMetadata == flagNoMetadata {
//		return ret
//	}
//
//	if ret.flags&flagGlobalTablesSpec == flagGlobalTablesSpec {
//		keyspace := frame.ReadString(b)
//		table := frame.ReadString(b)
//		ret.cols = readColsSpec(b, ret.colCnt, keyspace, table)
//	} else {
//		ret.cols = readColsUnspec(b, ret.colCnt)
//	}
//
//	return ret
//}
//
//type Row = []CQLValue
//
//type Rows struct {
//	metadata resultMetadata
//	rowCnt   frame.Int
//	rows     []Row
//}
//
//func ReadRows(b *bytes.Buffer) Rows {
//	ret := Rows{
//		metadata: readResultMetadata(b),
//		rowCnt:   frame.ReadInt(b),
//	}
//
//	ret.rows = make([]Row, ret.rowCnt)
//	for i := range ret.rows {
//		ret.rows[i] = make(Row, ret.metadata.colCnt)
//		for j := range ret.rows[i] {
//			// Is this bad style? foo[][] when foo's type is []Row
//			ret.rows[i][j] = readCQLValue(b, ret.metadata.cols[j])
//		}
//	}
//
//	return ret
//}
//
//func ReadSetKeyspace(b *bytes.Buffer) string {
//	return frame.ReadString(b)
//}
//
//type preparedMetadata struct {
//	flags     frame.Int
//	colCnt    frame.Int
//	pkCnt     frame.Int
//	pkIndexes []frame.Short
//	cols      []ColumnInfo
//}
//
//func readPreparedMetadata(b *bytes.Buffer) preparedMetadata {
//	ret := preparedMetadata{
//		flags:  frame.ReadInt(b),
//		colCnt: frame.ReadInt(b),
//		pkCnt:  frame.ReadInt(b),
//	}
//
//	ret.pkIndexes = make([]frame.Short, ret.pkCnt)
//	for i := range ret.pkIndexes {
//		ret.pkIndexes[i] = frame.ReadShort(b)
//	}
//
//	if ret.flags&flagGlobalTablesSpec == flagGlobalTablesSpec {
//		keyspace = frame.ReadString(b)
//		table = frame.ReadString(b)
//		ret.cols = readColsSpec(b, ret.colCnt, keyspace, table)
//	} else {
//		ret.cols = readColsUnspec(b, ret.colCnt)
//	}
//
//	return ret
//}
//
//type Prepared struct {
//	// FIXME: ShortBytes?
//	id             frame.Bytes
//	metadata       preparedMetadata
//	resultMetadata resultMetadata
//}
//
//func ReadPrepared(b *bytes.Buffer) Prepared {
//	return Prepared{
//		id:             frame.ReadBytes(b),
//		metadata:       readPreparedMetadata(b),
//		resultMetadata: readResultMetadata(b),
//	}
//}
//
//const (
//	flagGlobalTablesSpec frame.Int = 0x0001
//	flagHasMorePages     frame.Int = 0x0002
//	flagNoMetadata       frame.Int = 0x0004
//)
