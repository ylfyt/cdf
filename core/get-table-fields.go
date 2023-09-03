package core

import (
	"fmt"
	"strings"
)

type FieldInfo struct {
	Name string
	Type string
	Ref  struct {
		Table string
		Field string
	}
}

func getTableFields(dbName string, tableName string) map[string]*FieldInfo {
	db, exist := schema.Databases[dbName]
	if !exist {
		return nil
	}

	table, exist := db.Tables[tableName]
	if !exist {
		return nil
	}

	var result map[string]*FieldInfo = make(map[string]*FieldInfo)
	for fieldName := range table.Fields {
		field := table.Fields[fieldName]
		if field, ok := field.(string); ok {
			result[fieldName] = &FieldInfo{
				Name: fieldName,
				Type: field,
			}
			continue
		}

		if field, ok := field.(map[string]any); ok {
			fieldType := field["type"]
			if fieldType == nil {
				fmt.Println("Why nil", field)
				return nil
			}
			fieldInfo := FieldInfo{
				Name: fieldName,
				Type: fmt.Sprint(fieldType),
			}
			fieldRef := field["ref"]
			if ref, ok := fieldRef.(string); ok {
				dataRef := strings.Split(ref, ".")
				if len(dataRef) < 2 {
					fmt.Println("Why < 2", dataRef)
					return nil
				}
				fieldInfo.Ref.Field = dataRef[1]
				fieldInfo.Ref.Table = dataRef[0]
			}
			result[fieldName] = &fieldInfo
			continue
		}
		fmt.Println("???", field)
	}

	return result
}
