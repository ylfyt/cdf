package core

import (
	"cdf/models"
	"cdf/utils"
	"errors"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

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

	// === AUTH
	dbRules := deleteAuthRules[db.Name]
	if len(dbRules) != 0 {
		err := me.validateRules(dbRules, db.Name, "", nil, nil, false)
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
			dataTmp, err := db.read(db.Conn, &models.QueryTable{
				Name:         tableName,
				SelectFields: map[string]any{},
			}, wheres)
			if err != nil {
				return 0, err
			}
			data = dataTmp
		}
		err := me.validateRules(tableRules, db.Name, tableName, nil, data, false)
		if err != nil {
			return 0, err
		}
	}
	for fieldName := range schema.Databases[db.Name].Tables[tableName].Fields {
		fieldRules := deleteAuthRules[db.Name+"."+tableName+"."+fieldName]
		if len(fieldRules) != 0 {
			err := me.validateRules(fieldRules, db.Name, tableName, nil, nil, false)
			if err != nil {
				return 0, err
			}
		}
	}
	// === END AUTH

	return db.delete(db.Conn, tableName, wheres)
}
