package core

import (
	"errors"
	"fmt"
	"github.com/xwb1989/sqlparser"
)

func executeInsert(stmt *sqlparser.Insert) error {
	columns := make([]string, 0)
	for _, column := range stmt.Columns {
		columns = append(columns, column.CompliantName())
	}

	insertValues, ok := stmt.Rows.(sqlparser.Values)
	if !ok {
		return errors.New("no values found in the insert statement")
	}

	values := [][]any{}

	for _, tuple := range insertValues {
		if len(tuple) != len(columns) {
			return errors.New("tuple count should be the same with column number")
		}

		value := []any{}
		for _, val := range tuple {
			switch val := val.(type) {
			case *sqlparser.SQLVal:
				value = append(value, string(val.Val))
			case *sqlparser.NullVal:
				value = append(value, nil)
			default:
				return errors.New("unsupported value type")
			}
		}

		values = append(values, value)
	}

	tableName := stmt.Table.Name.CompliantName()

	db := getDb(tableName)
	if db == nil {
		return fmt.Errorf("table %s not found", tableName)
	}
	driver := drivers[db.Type]
	return driver.insert(db.Conn, tableName, columns, values)
}

func Execute(stmt sqlparser.Statement) (any, error) {
	if insertStmt, ok := stmt.(*sqlparser.Insert); ok {
		return nil, executeInsert(insertStmt)
	}

	return nil, fmt.Errorf("unsupported statement")
}
