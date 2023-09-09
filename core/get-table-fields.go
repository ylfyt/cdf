package core

import (
	"cdf/models"
	"fmt"
	"strings"
)

func getTableFields(dbName string, tableName string) map[string]*models.FieldInfo {
	db, exist := schema.Databases[dbName]
	if !exist {
		return nil
	}

	table, exist := db.Tables[tableName]
	if !exist {
		return nil
	}

	var result map[string]*models.FieldInfo = make(map[string]*models.FieldInfo)
	for fieldName := range table.Fields {
		field := table.Fields[fieldName]
		if field, ok := field.(string); ok {
			result[fieldName] = &models.FieldInfo{
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
			fieldInfo := models.FieldInfo{
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
