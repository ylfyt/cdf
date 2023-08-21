package core

import (
	"cdf/models"
	"cdf/utils"
	"errors"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func getColumnValuesFromWhere(expr sqlparser.Expr) map[string]any {
	values := make(map[string]any)

	// If the expression is a binary comparison
	if binExpr, ok := expr.(*sqlparser.ComparisonExpr); ok {
		leftCol, ok := binExpr.Left.(*sqlparser.ColName)
		if ok {
			colName := leftCol.Name.String()
			val, _ := utils.ParseValue(binExpr.Right)

			// Add column name and value to the map
			values[colName] = val
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

func (me *Handler) deleteAction(stmt *sqlparser.Delete) (int, error) {
	var wheres []*models.Cond
	if stmt.Where != nil {
		wheres = utils.ParseJoinCondition(stmt.Where.Expr)
	}

	if len(wheres) == 0 {
		return 0, errors.New("deleting table without where expr is not allowed")
	}

	if len(stmt.TableExprs) == 0 {
		return 0, errors.New("table name is not found")
	}

	tableName := ""
	if expr, ok := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr); ok {
		if table, ok := expr.Expr.(sqlparser.TableName); ok {
			tableName = table.Name.CompliantName()
		}
	}

	if tableName == "" {
		return 0, errors.New("table name is not found")
	}

	db := getDb(tableName)
	if db == nil {
		return 0, fmt.Errorf("table %s not found", tableName)
	}
	driver := drivers[db.Type]

	// === AUTH
	dbRules := deleteAuthRules[db.Name]
	if len(dbRules) != 0 {
		err := me.validateRules(dbRules, db.Name, "", nil, nil, nil)
		if err != nil {
			return 0, err
		}
	}
	tableRules := deleteAuthRules[db.Name+"."+tableName]
	if len(tableRules) != 0 {
		isDataRequired := false
		for _, rule := range tableRules {
			for key := range rule {
				if strings.HasPrefix(key, "$") {
					isDataRequired = true
				}
			}
		}
		var data []map[string]any
		if isDataRequired {
			dataTmp, err := driver.read(db.Conn, &models.QueryTable{
				Name:         tableName,
				SelectFields: map[string]any{},
			}, wheres)
			if err != nil {
				return 0, err
			}
			data = dataTmp
		}
		err := me.validateRules(tableRules, db.Name, tableName, utils.GetMapKeys(schema.Databases[db.Name].Tables[tableName].Fields), nil, data)
		if err != nil {
			return 0, err
		}
	}
	return driver.delete(db.Conn, tableName, wheres)
}
