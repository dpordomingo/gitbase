package metadata

import (
	"fmt"
	"io"

	"github.com/gitql/gitql/memory"
	"github.com/gitql/gitql/sql"
)

var metadataColumns = []metadataColumn{
	metadataColumn{sql.Column{"table_catalog", sql.String}, "def"},
	metadataColumn{sql.Column{"table_schema", sql.String}, "nil"},
	metadataColumn{sql.Column{"table_name", sql.String}, "nil"},
	metadataColumn{sql.Column{"column_name", sql.String}, "nil"},
	metadataColumn{sql.Column{"ordinal_position", sql.String}, "nil"},
	metadataColumn{sql.Column{"column_default", sql.String}, "nil"},
	metadataColumn{sql.Column{"is_nullable", sql.String}, "nil"},
	metadataColumn{sql.Column{"data_type", sql.String}, "nil"},
	//metadataColumn{sql.Column{"character_maximum_length", sql.BigInteger}, int64(0)},
	//metadataColumn{sql.Column{"character_octet_length", sql.BigInteger}, int64(0)},
	//metadataColumn{sql.Column{"numeric_precision", sql.BigInteger}, int64(0)},
	//metadataColumn{sql.Column{"numeric_scale", sql.BigInteger}, int64(0)},
	//metadataColumn{sql.Column{"datetime_precision", sql.BigInteger}, int64(0)},
	metadataColumn{sql.Column{"character_set_name", sql.String}, "nil"},
	metadataColumn{sql.Column{"collation_name", sql.String}, "nil"},
	//metadataColumn{sql.Column{"column_type", sql.String}, "nil"},
	metadataColumn{sql.Column{"column_key", sql.String}, "nil"},
	metadataColumn{sql.Column{"extra", sql.String}, "nil"},
	//metadataColumn{sql.Column{"privileges", sql.String}, "nil"},
	metadataColumn{sql.Column{"column_comment", sql.String}, "nil"},
	//metadataColumn{sql.Column{"generation_expression", sql.String}, "nil"},
}

type columnsTable struct {
	*memory.Table
	index map[string]int
}

func newcolumnsTable(catalog sql.DBStorer) *columnsTable {
	schema, index := schema(metadataColumns)
	data := columnsData{data: catalog, index: index}
	return &columnsTable{
		memory.NewTable(SchemaColumnTableName, schema, data),
		index,
	}
}

func (t *columnsTable) Insert(values ...interface{}) error {
	return fmt.Errorf("ERROR: %s is a table view; Insertion is not allowed", t.Name())
}

type columnsData struct {
	data  sql.DBStorer
	index map[string]int
}

func (c columnsData) IterData() memory.IteratorData {
	return &columnsDBIter{data: c.data.Dbs(), index: c.index}
}

func (c columnsData) Insert(values ...interface{}) error {
	return fmt.Errorf("ERROR: Insertion is not allowed")
}

type columnsDBIter struct {
	data  []sql.Database
	index map[string]int
	cur   internalTableColumnIterator
	idx   int
	count *int
}

func (i *columnsDBIter) Length() int {
	if i.count == nil {
		count := 0
		for _, db := range i.data {
			tables := db.Tables()
			for _, t := range tables {
				count += len(t.Schema())
			}
		}
		i.count = &count
	}
	return *i.count
}

func (i *columnsDBIter) Get(idx int) []interface{} {
	next, _ := i.Next()
	return next
}

func (i *columnsDBIter) Next() ([]interface{}, error) {
	if i.cur == nil {
		i.cur = &cTblIterator{
			data: tables(i.data[i.idx].Tables()),
		}
	}

	if table, column, err := i.cur.Next(); err == nil {
		return i.row(i.data[i.idx], table, column), nil
	} else if i.idx < len(i.data) {
		i.cur = nil
		i.idx++
		return i.Next()
	}

	return nil, io.EOF
}

func (i *columnsDBIter) row(db sql.Database, table sql.Table, column sql.Column) []interface{} {
	row := make([]interface{}, len(metadataColumns))
	k := 0
	for pos, f := range metadataColumns {
		row[k] = i.getColumn(f.Name, db, table, column, pos)
		k++
	}

	return row
}

func (i *columnsDBIter) getColumn(name string, db sql.Database, table sql.Table, column sql.Column, pos0 int) interface{} {
	switch name {
	case "table_schema":
		return db.Name()
	case "table_name":
		return table.Name()
	case "column_name":
		return column.Name
	case "ordinal_position":
		return pos0 + 1
	}
	return metadataColumns[i.index[name]].def
}

type internalTableColumnIterator interface {
	Next() (sql.Table, sql.Column, error)
}

type cTblIterator struct {
	data []sql.Table
	cur  internalColumnIterator
	idx  int
}

func (i *cTblIterator) Next() (sql.Table, sql.Column, error) {
	if i.cur == nil {
		i.cur = &cIterator{data: i.data[i.idx].Schema()}
	}

	if column, err := i.cur.Next(); err == nil {
		return i.data[i.idx], column, nil
	} else if i.idx < len(i.data)-1 {
		i.cur = nil
		i.idx++
		return i.Next()
	}

	return nil, sql.Column{}, io.EOF
}

type internalColumnIterator interface {
	Next() (sql.Column, error)
}

type cIterator struct {
	data sql.Schema
	idx  int
}

func (i *cIterator) Next() (sql.Column, error) {
	if i.idx >= len(i.data) {
		return sql.Column{}, io.EOF
	}

	i.idx++
	return i.data[i.idx-1], nil
}
