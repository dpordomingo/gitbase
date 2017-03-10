package main

import (
	"database/sql"
	"os"
	"regexp"

	"gopkg.in/sqle/gitql.v0"
	"gopkg.in/sqle/gitql.v0/internal/format"

	"gopkg.in/sqle/sqle.v0"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/utils/ioutil"
)

var gitDBname = "gitql"

var dbNameRegExp = regexp.MustCompile(`(?i)select\s+[\s\S]+?\s+from\s+([^\s\.]+)\.`)

type cmdQueryBase struct {
	cmd

	Path string `short:"p" long:"path" description:"Path where the git repository is located"`

	db   *sql.DB
	name string
}

func (c *cmdQueryBase) buildDatabase() error {
	c.print("opening %q repository...\n", c.Path)

	var err error
	r, err := git.PlainOpen(c.Path)
	if err != nil {
		return err
	}

	c.name = gitDBname
	if err := sqle.DefaultEngine.AddDatabase(gitquery.NewDatabase(gitDBname, r)); err != nil {
		return err
	}

	c.db, err = sql.Open(sqle.DriverName, "")
	return err
}

func (c *cmdQueryBase) executeQuery(sql string) (*sql.Rows, error) {
	if currentDatabaseName := readDBName(sql); currentDatabaseName != "" {
		if err := sqle.DefaultEngine.CurrentDatabase(currentDatabaseName); err != nil {
			return nil, err
		}
	}

	c.print("executing %q at %q\n", sql, c.name)
	return c.db.Query(sql)
}

func (c *cmdQueryBase) printQuery(rows *sql.Rows, formatId string) (err error) {
	defer ioutil.CheckClose(rows, &err)

	f, err := format.NewFormat(formatId, os.Stdout)
	if err != nil {
		return err
	}
	defer ioutil.CheckClose(f, &err)

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	if err := f.WriteHeader(cols); err != nil {
		return err
	}

	vals := make([]interface{}, len(cols))
	valPtrs := make([]interface{}, len(cols))
	for i := 0; i < len(cols); i++ {
		valPtrs[i] = &vals[i]
	}

	for {
		if !rows.Next() {
			break
		}

		if err := rows.Scan(valPtrs...); err != nil {
			return err
		}

		if err := f.Write(vals); err != nil {
			return err
		}
	}

	return rows.Err()
}

func readDBName(queryString string) string {
	matches := dbNameRegExp.FindStringSubmatch(queryString)
	if len(matches) > 1 {
		return matches[1]
	}

	return ""
}
