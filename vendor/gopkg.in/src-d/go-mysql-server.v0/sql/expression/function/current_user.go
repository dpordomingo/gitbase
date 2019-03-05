package function

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// CurrentUser stands for CURRENT_USER() function
type CurrentUser struct {
	user string
}

// NewCurrentUser returns a new CurrentUser function
func NewCurrentUser(user string) func() sql.Expression {
	return func() sql.Expression {
		return &CurrentUser{user}
	}
}

// Type implements the sql.Expression (sql.Text)
func (db *CurrentUser) Type() sql.Type { return sql.Text }

// IsNullable implements the sql.Expression interface.
// The function returns always true
func (db *CurrentUser) IsNullable() bool {
	return true
}

func (*CurrentUser) String() string {
	return "CURRENT_USER()"
}

// TransformUp implements the sql.Expression interface.
func (db *CurrentUser) TransformUp(fn sql.TransformExprFunc) (sql.Expression, error) {
	return fn(db)
}

// Resolved implements the sql.Expression interface.
func (db *CurrentUser) Resolved() bool {
	return true
}

// Children implements the sql.Expression interface.
func (db *CurrentUser) Children() []sql.Expression { return nil }

// Eval implements the sql.Expression interface.
func (db *CurrentUser) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return db.user, nil
}
