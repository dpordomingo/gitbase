package metadata

import (
	"fmt"

	"github.com/gitql/gitql/memory"
	"github.com/gitql/gitql/sql"
)

const (
	//SchemaDBname is the name of the sql.Table used to store catalog metadata
	SchemaDBname = "INFORMATION_SCHEMA"

	//SchemaDBTableName is the name of the Databases metadata table
	SchemaDBTableName = "SCHEMATA"

	//SchemaTableTableName is the name of the Tables metadata table
	SchemaTableTableName = "TABLES"

	//SchemaColumnTableName is the name of the Columns metadata table
	SchemaColumnTableName = "COLUMNS"
)

type metadataDB struct {
	memory.Database
	catalog sql.DBStorer
}

func NewDB(catalog sql.DBStorer) sql.Database {
	embeddedDB := memory.NewDatabase(SchemaDBname)
	m := &metadataDB{
		Database: *embeddedDB,
		catalog:  catalog,
	}

	m.addTable(newSchemataTable(catalog))
	m.addTable(newTablesTable(catalog))
	m.addTable(newcolumnsTable(catalog))
	return m
}

func (d metadataDB) AddTable(t *table) {
	panic(fmt.Sprintf("The Database %s is readonly", d.Name()))
}

func (d metadataDB) addTable(t sql.Table) {
	d.Database.AddTable(t)
}
