package core

import (
	"cdf/models"
	"cdf/utils"
	"errors"
	"fmt"
	"strings"

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

	// === AUTH
	dbRules := updateAuthRules[db.Name]
	if len(dbRules) != 0 {
		err := me.validateRules(dbRules, db.Name, "", nil, nil)
		if err != nil {
			return 0, err
		}
	}
	inputValues := []map[string]any{
		values,
	}
	tableRules := updateAuthRules[db.Name+"."+tableName]
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
		err := me.validateRules(tableRules, db.Name, tableName, inputValues, data)
		if err != nil {
			return 0, err
		}
	}
	for fieldName := range schema.Databases[db.Name].Tables[tableName].Fields {
		fieldRules := updateAuthRules[db.Name+"."+tableName+"."+fieldName]
		if len(fieldRules) != 0 {
			err := me.validateRules(fieldRules, db.Name, tableName, inputValues, nil)
			if err != nil {
				return 0, err
			}
		}
	}
	// === END AUTH
	return driver.update(db.Conn, tableName, wheres, values)
}
