package metadata

import (
	"fmt"

	"gopkg.in/sqle/sqle.v0/memory"
	"gopkg.in/sqle/sqle.v0/sql"
)

type metadataColumn struct {
	sql.Column
	def interface{}
}

func schema(columns []metadataColumn) (schema sql.Schema, index map[string]int) {
	schema = make([]sql.Column, len(columns))
	index = make(map[string]int)
	i := 0
	for _, f := range columns {
		schema[i] = f.Column
		index[f.Name] = i
		i++
	}

	return schema, index
}

type table struct {
	*memory.Table
}

func newTable(name string, schema sql.Schema, data memory.TableData) *table {
	return &table{
		memory.NewTable(name, schema, data),
	}
}

func (t *table) Insert(values ...interface{}) error {
	panic(fmt.Sprintf("The Database %s is readonly", t.Name()))
}
