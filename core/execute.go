package core

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
)

func Execute(stmt sqlparser.Statement) (any, error) {
	if stmt, ok := stmt.(*sqlparser.Insert); ok {
		return nil, insertAction(stmt)
	}

	if stmt, ok := stmt.(*sqlparser.Select); ok {
		return selectAction(stmt)
	}

	if stmt, ok := stmt.(*sqlparser.Delete); ok {
		return deleteAction(stmt)
	}

	if stmt, ok := stmt.(*sqlparser.Update); ok {
		return updateAction(stmt)
	}

	return nil, fmt.Errorf("unsupported statement")
}
