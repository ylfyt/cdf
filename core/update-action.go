package core

import (
	"cdf/models"
	"cdf/utils"
	"errors"
	"fmt"

	"github.com/xwb1989/sqlparser"
)

func (me *Handler) updateAction(stmt *sqlparser.Update) (any, error) {
	var wheres []*models.Cond
	if stmt.Where != nil {
		wheres = utils.ParseJoinCondition(stmt.Where.Expr)
	}
	if len(wheres) == 0 {
		return nil, errors.New("updating table without where expr is not allowed")
	}

	if len(stmt.TableExprs) == 0 {
		return nil, errors.New("table name is not found")
	}

	tableName := ""
	if expr, ok := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr); ok {
		if table, ok := expr.Expr.(sqlparser.TableName); ok {
			tableName = table.Name.CompliantName()
		}
	}

	if tableName == "" {
		return nil, errors.New("table name is not found")
	}

	values := map[string]any{}
	for _, updateExpr := range stmt.Exprs {
		val, err := utils.ParseValue(updateExpr.Expr)
		if err != nil {
			return nil, err
		}
		values[updateExpr.Name.Name.String()] = val
	}

	db := getDb(tableName)
	if db == nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	driver := drivers[db.Type]
	return driver.update(db.Conn, tableName, wheres, values)
}
