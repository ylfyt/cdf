package core

import (
	"cdf/utils"
	"errors"
	"fmt"

	"github.com/xwb1989/sqlparser"
)

func (me *Handler) insertAction(stmt *sqlparser.Insert) error {
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
			parsedVal, err := utils.ParseValue(val)
			if err != nil {
				return err
			}
			value = append(value, parsedVal)
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
