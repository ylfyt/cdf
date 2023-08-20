package core

import (
	"cdf/utils"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func isEQ[T comparable](a T, b T) bool {
	return a == b
}

type iCompare interface {
	int32 | int64 | float32 | float64
}

func isGT[T iCompare](a T, b T) bool {
	return a > b
}
func isGTE[T iCompare](a T, b T) bool {
	return a >= b
}
func isLT[T iCompare](a T, b T) bool {
	return a < b
}
func isLTE[T iCompare](a T, b T) bool {
	return a <= b
}

func compare[T comparable](a T, b T, op string) bool {
	switch op {
	case "=":
		return a == b
	case "$gt":

	}

	return false
}

func isValid(a any, b any, op string) bool {
	return a == b
	// switch a := a.(type) {
	// case int:

	// }

	return false
}

func validateInsert(rules []map[string]any, columns []string, values [][]any) error {
	getFieldIdx := func(columns []string, field string) int {
		for idx, col := range columns {
			if col == field {
				return idx
			}
		}
		return -1
	}

	for _, rule := range rules {
		for key, authVal := range rule {
			if strings.HasPrefix(key, "data.") {
				field := strings.Split(key, ".")[1]
				if val, ok := authVal.(map[string]any); ok {
					fmt.Printf("Data: %+v\n", val)
					continue
				}

				idx := getFieldIdx(columns, field)
				if idx == -1 {
					return fmt.Errorf("data field %s not found", field)
				}
				for _, dataValues := range values {
					dataValue := dataValues[idx]
					fmt.Println("Compare", dataValue, authVal, reflect.TypeOf(dataValue), reflect.TypeOf(authVal))
					if isValid(dataValue, authVal, "=") {
						return fmt.Errorf("not valid 2")
					}
				}
			}
		}
	}

	return nil
}

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
	// TODO: auth db

	rules := createAuthRules[db.Name+"."+tableName]
	if len(rules) != 0 {
		err := validateInsert(rules, columns, values)
		if err != nil {
			return err
		}
	}

	driver := drivers[db.Type]
	return driver.insert(db.Conn, tableName, columns, values)
}
