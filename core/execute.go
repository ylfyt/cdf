package core

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
)

type Handler struct {
	Claim map[string]any
}

func (me *Handler) Execute(stmt sqlparser.Statement) (any, error) {
	if stmt, ok := stmt.(*sqlparser.Insert); ok {
		return nil, me.insertAction(stmt)
	}

	if stmt, ok := stmt.(*sqlparser.Select); ok {
		return me.selectAction(stmt)
	}

	if stmt, ok := stmt.(*sqlparser.Delete); ok {
		return me.deleteAction(stmt)
	}

	if stmt, ok := stmt.(*sqlparser.Update); ok {
		return me.updateAction(stmt)
	}

	return nil, fmt.Errorf("unsupported statement")
}
