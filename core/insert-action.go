package core

import (
	"cdf/utils"
	"errors"
	"fmt"
	"reflect"
	"strings"

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

func isValid(val1 any, val2 any, fieldType string, op string) error {
	if fieldType == "int" {
		a, _ := utils.CaseInt64(val1)
		if op == "$in" || op == "$nin" {
			if _, ok := val2.([]any); !ok {
				return fmt.Errorf("val2 %s is not array", utils.CaseString(val2))
			}
			vals := val2.([]any)
			found := false
			for _, val := range vals {
				b, _ := utils.CaseInt64(val)
				if op == "$in" && a == b {
					found = true
					break
				}
				if op == "$nin" && a == b {
					return fmt.Errorf("%s in %s", utils.CaseString(val1), utils.CaseString(val2))
				}
			}
			if op == "$in" && !found {
				return fmt.Errorf("%s not in %s", utils.CaseString(val1), utils.CaseString(val2))
			}
			return nil
		}
		b, _ := utils.CaseInt64(val2)
		switch op {
		case "$eq":
			if a != b {
				return fmt.Errorf("%s not equal %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$neq":
			if a == b {
				return fmt.Errorf("%s equal %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$gt":
			if !(a > b) {
				return fmt.Errorf("%s not gte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$gte":
			if !(a >= b) {
				return fmt.Errorf("%s not gte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$lt":
			if !(a < b) {
				return fmt.Errorf("%s not lt %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$lte":
			if !(a <= b) {
				return fmt.Errorf("%s not lte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$in":
		default:
			return fmt.Errorf("operator %s not supported", op)
		}
	} else if fieldType == "float" {
		a, _ := utils.CaseFloat64(val1)
		if op == "$in" || op == "$nin" {
			if _, ok := val2.([]any); !ok {
				return fmt.Errorf("val2 %s is not array", utils.CaseString(val2))
			}
			vals := val2.([]any)
			found := false
			for _, val := range vals {
				b, _ := utils.CaseFloat64(val)
				if op == "$in" && a == b {
					found = true
					break
				}
				if op == "$nin" && a == b {
					return fmt.Errorf("in %s | %s", utils.CaseString(val1), utils.CaseString(val2))
				}
			}
			if op == "$in" && !found {
				return fmt.Errorf("not in %s | %s", utils.CaseString(val1), utils.CaseString(val2))
			}
			return nil
		}
		b, _ := utils.CaseFloat64(val2)
		switch op {
		case "$eq":
			if a != b {
				return fmt.Errorf("%s not equal %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$neq":
			if a == b {
				return fmt.Errorf("%s equal %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$gt":
			if !(a > b) {
				return fmt.Errorf("%s not gte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$gte":
			if !(a >= b) {
				return fmt.Errorf("%s not gte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$lt":
			if !(a < b) {
				return fmt.Errorf("%s not lt %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$lte":
			if !(a <= b) {
				return fmt.Errorf("%s not lte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$in":
		default:
			return fmt.Errorf("operator %s not supported", op)
		}
	} else if fieldType == "string" {
		a := utils.CaseString(val1)
		if op == "$in" || op == "$nin" {
			if _, ok := val2.([]any); !ok {
				return fmt.Errorf("val2 %s is not array", utils.CaseString(val2))
			}
			vals := val2.([]any)
			found := false
			for _, val := range vals {
				b := utils.CaseString(val)
				if op == "$in" && a == b {
					found = true
					break
				}
				if op == "$nin" && a == b {
					return fmt.Errorf("in %s | %s", utils.CaseString(val1), utils.CaseString(val2))
				}
			}
			if op == "$in" && !found {
				return fmt.Errorf("not in %s | %s", utils.CaseString(val1), utils.CaseString(val2))
			}
			return nil
		}
		b := utils.CaseString(val2)
		switch op {
		case "$eq":
			if a != b {
				return fmt.Errorf("%s not equal %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$neq":
			if a == b {
				return fmt.Errorf("%s equal %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$gt":
			if !(a > b) {
				return fmt.Errorf("%s not gte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$gte":
			if !(a >= b) {
				return fmt.Errorf("%s not gte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$lt":
			if !(a < b) {
				return fmt.Errorf("%s not lt %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$lte":
			if !(a <= b) {
				return fmt.Errorf("%s not lte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$in":
		default:
			return fmt.Errorf("operator %s not supported", op)
		}
	} else {
		return fmt.Errorf("field type %s not supported", fieldType)
	}

	return nil
}

func (me *Handler) validateRules(rules []map[string]any, dbName string, tableName string, inputValues []map[string]any, existValues []map[string]any) error {
	for _, rule := range rules {
		for key, authRule := range rule {
			if rule, ok := authRule.(string); ok && strings.HasPrefix(rule, "data.") {
				tmp := key
				key = rule
				authRule = tmp
			}
			if strings.HasPrefix(key, "data.") {
				if tableName == "" {
					return fmt.Errorf("in db ctx")
				}
				field := strings.Split(key, ".")[1]
				fieldType := getFieldType(dbName, tableName, field)
				if fieldType == "" {
					return fmt.Errorf("field %s not found", field)
				}

				for _, values := range inputValues {
					dataValue, exist := values[field]
					_ = exist
					// if !exist {
					// 	return fmt.Errorf("input field %s not found", field)
					// }

					if val, ok := authRule.(map[string]any); ok {
						for op, val := range val {
							err := isValid(dataValue, val, fieldType, op)
							if err != nil {
								return err
							}
						}
						continue
					}
					if val, ok := authRule.(string); ok && strings.HasPrefix(val, "auth.") {
						claimField := strings.Split(val, ".")[1]
						if me.Claim == nil {
							return fmt.Errorf("unauth")
						}
						authVal, exist := me.Claim[claimField]
						if !exist {
							return fmt.Errorf("claim field %s not found", claimField)
						}

						err := isValid(dataValue, authVal, fieldType, "$eq")
						if err != nil {
							return err
						}
						continue
					}

					err := isValid(dataValue, authRule, fieldType, "$eq")
					if err != nil {
						return err
					}
				}

				continue
			}

			if strings.HasPrefix(key, "auth.") {
				claimField := strings.Split(key, ".")[1]
				if me.Claim == nil {
					return fmt.Errorf("unauth")
				}
				claimVal, exist := me.Claim[claimField]
				if !exist {
					return fmt.Errorf("claim field %s not found", claimField)
				}
				claimType := getValueType(claimVal)
				if mapVal, ok := authRule.(map[string]any); ok {
					for op, val := range mapVal {
						err := isValid(claimVal, val, claimType, op)
						if err != nil {
							return err
						}
					}
					continue
				}
				err := isValid(claimVal, authRule, claimType, "$eq")
				if err != nil {
					return err
				}
				continue
			}

			if strings.HasPrefix(key, "$") {
				if tableName == "" {
					return fmt.Errorf("in db ctx")
				}
				field := strings.Split(key, "$")[1]
				fieldType := getFieldType(dbName, tableName, field)
				if fieldType == "" {
					return fmt.Errorf("field %s not found", field)
				}
				for _, values := range existValues {
					fieldVal, exist := values[field]
					if !exist {
						return fmt.Errorf("field %s is not found", field)
					}
					if val, ok := authRule.(map[string]any); ok {
						for op, val := range val {
							err := isValid(fieldVal, val, fieldType, op)
							if err != nil {
								return err
							}
						}
						continue
					}
					if val, ok := authRule.(string); ok && strings.HasPrefix(val, "auth.") {
						claimField := strings.Split(val, ".")[1]
						if me.Claim == nil {
							return fmt.Errorf("unauth")
						}
						authVal, exist := me.Claim[claimField]
						if !exist {
							return fmt.Errorf("claim field %s not found", claimField)
						}

						err := isValid(fieldVal, authVal, fieldType, "$eq")
						if err != nil {
							return err
						}
						continue
					}
					err := isValid(fieldVal, authRule, fieldType, "$eq")
					if err != nil {
						return err
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
