package core

import (
	"cdf/utils"
	"errors"
	"fmt"
	"reflect"

	"github.com/xwb1989/sqlparser"
)

var (
	TYPE_INT    = "int"
	TYPE_STRING = "string"
	TYPE_FLOAT  = "float"
)

func getValueType(a any) string {
	switch reflect.TypeOf(a).Kind() {
	case reflect.String:
		return TYPE_STRING
	case reflect.Int, reflect.Int32, reflect.Int64:
		return TYPE_INT
	case reflect.Float32, reflect.Float64:
		return TYPE_FLOAT
	}
	return ""
}

func getFieldType(dbName string, tableName string, fieldName string) string {
	db, exist := schema.Databases[dbName]
	if !exist {
		return ""
	}
	table, exist := db.Tables[tableName]
	if !exist {
		return ""
	}
	fieldInfo, exist := table.Fields[fieldName]
	if !exist {
		return ""
	}
	if field, ok := fieldInfo.(map[string]any); ok {
		fieldType := field["type"]
		if fieldType == nil {
			return ""
		}
		return fmt.Sprint(fieldType)
	}
	return fmt.Sprint(fieldInfo)
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

	inputValues := []map[string]any{}
	for _, value := range values {
		newValue := map[string]any{}
		for idx, column := range columns {
			newValue[column] = value[idx]
		}
		inputValues = append(inputValues, newValue)
	}

	// === AUTH
	dbRules := createAuthRules[db.Name]
	if len(dbRules) != 0 {
		err := me.validateRules(dbRules, db.Name, "", nil, nil)
		if err != nil {
			return err
		}
	}
	tableRules := createAuthRules[db.Name+"."+tableName]
	if len(tableRules) != 0 {
		err := me.validateRules(tableRules, db.Name, tableName, inputValues, nil)
		if err != nil {
			return err
		}
	}
	for fieldName := range schema.Databases[db.Name].Tables[tableName].Fields {
		fieldRules := createAuthRules[db.Name+"."+tableName+"."+fieldName]
		if len(fieldRules) != 0 {
			err := me.validateRules(fieldRules, db.Name, tableName, inputValues, nil)
			if err != nil {
				return err
			}
		}
	}
	// === END AUTH

	driver := drivers[db.Type]
	return driver.insert(db.Conn, tableName, columns, values)
}
