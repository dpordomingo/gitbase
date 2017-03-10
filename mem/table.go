package mem

import (
	"fmt"
	"io"

	"gopkg.in/sqle/sqle.v0/memory"
	"gopkg.in/sqle/sqle.v0/sql"
)

type Table struct {
	name   string
	schema sql.Schema
	data   [][]interface{}
}

func NewTable(name string, schema sql.Schema) *Table {
	return &Table{
		name:   name,
		schema: schema,
	}
}

func (Table) Resolved() bool {
	return true
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) Schema() sql.Schema {
	return t.schema
}

func (t *Table) Children() []sql.Node {
	return []sql.Node{}
}

func (t *Table) RowIter() (sql.RowIter, error) {
	return memory.NewIter(&container{data: t.data}), nil
}

func (t *Table) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(t)
}

func (t *Table) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return t
}

func (t *Table) Insert(row sql.Row) error {
	if len(row) != len(t.schema) {
		return fmt.Errorf("insert expected %d values, got %d", len(t.schema), len(row))
	}

	for idx, value := range row {
		f := t.schema[idx]
		if !f.Type.Check(value) {
			return sql.ErrInvalidType
		}
	}

	crow := make([]interface{}, len(row))
	for i, col := range row {
		crow[i] = col
	}
	t.data = append(t.data, crow)
	return nil
}

type iter struct {
	idx  int
	rows []sql.Row
}

func (i *iter) Next() (sql.Row, error) {
	if i.idx >= len(i.rows) {
		return nil, io.EOF
	}

	row := i.rows[i.idx]
	i.idx++
	return row.Copy(), nil
}

func (i *iter) Close() error {
	i.rows = nil
	return nil
}

type container struct {
	data [][]interface{}
}

func (i *container) Get(idx int) []interface{} {
	return i.data[idx]
}

func (i *container) Length() int {
	return len(i.data)
}
