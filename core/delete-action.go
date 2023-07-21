package core

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func getColumnValuesFromWhere(expr sqlparser.Expr) map[string]string {
	values := make(map[string]string)

	// If the expression is a binary comparison
	if binExpr, ok := expr.(*sqlparser.ComparisonExpr); ok {
		leftCol, ok := binExpr.Left.(*sqlparser.ColName)
		if ok {
			colName := leftCol.Name.String()
			value := sqlparser.String(binExpr.Right)

			value = strings.Trim(value, `'`)
			// Add column name and value to the map
			values[colName] = value
		}
	}

	// If the expression is a logical AND expression
	if andExpr, ok := expr.(*sqlparser.AndExpr); ok {
		// Recursively process both sides of the AND expression
		leftValues := getColumnValuesFromWhere(andExpr.Left)
		rightValues := getColumnValuesFromWhere(andExpr.Right)

		// Merge the values from both sides
		for col, val := range leftValues {
			values[col] = val
		}
		for col, val := range rightValues {
			values[col] = val
		}
	}

	return values
}

func deleteAction(stmt *sqlparser.Delete) (any, error) {
	wheres := map[string]string{}
	if stmt.Where != nil {
		wheres = getColumnValuesFromWhere(stmt.Where.Expr)
	}

	if len(wheres) == 0 {
		return nil, errors.New("deleting table without where expr is not allowed")
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

	db := getDb(tableName)
	if db == nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	driver := drivers[db.Type]
	return driver.delete(db.Conn, tableName, wheres)
}
