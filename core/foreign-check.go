package core

import (
	"cdf/models"
	"fmt"
)

func foreignCheck(fields map[string]*models.FieldInfo, columns []string, values [][]any, isFull bool) error {
	for fieldName, field := range fields {
		if field.Ref.Table == "" {
			continue
		}
		colIdx := -1
		for idx, col := range columns {
			if col == fieldName {
				colIdx = idx
				break
			}
		}
		if colIdx == -1 && !isFull {
			continue
		}
		if colIdx == -1 {
			return fmt.Errorf("field '%s' is reference field to %+v. it cannot be null", fieldName, field.Ref)
		}
		for _, value := range values {
			if value[colIdx] == nil {
				return fmt.Errorf("field '%s' is reference field to %+v. it cannot be null", fieldName, field.Ref)
			}
		}
	}

	type ForeignField struct {
		Field string
		Value any
	}
	var foreigns []map[string][]ForeignField = make([]map[string][]ForeignField, 0)

	for _, value := range values {
		// tableName -> fields
		var foreignMap map[string][]ForeignField = make(map[string][]ForeignField)
		for colIdx, col := range columns {
			field := fields[col]
			if field == nil {
				return fmt.Errorf("field %s is not found", col)
			}
			if field.Ref.Table == "" {
				continue
			}

			foreignMap[field.Ref.Table] = append(foreignMap[field.Ref.Table], ForeignField{
				Field: field.Ref.Field,
				Value: value[colIdx],
			})
		}
		if len(foreignMap) == 0 {
			continue
		}
		foreigns = append(foreigns, foreignMap)
	}

	for _, foreign := range foreigns {
		for table, fields := range foreign {
			db := getDb(table)
			if db == nil {
				return fmt.Errorf("table foreign '%s' is not found", table)
			}
			query := models.QueryTable{
				Name: table,
			}
			wheres := []*models.Cond{}
			for _, field := range fields {
				wheres = append(wheres, &models.Cond{
					Left: models.CondInfo{
						Field: field.Field,
					},
					Op: "=",
					Right: models.CondInfo{
						Value: field.Value,
					},
				})
			}
			res, err := db.read(db.Conn, &query, wheres)
			if err != nil {
				return err
			}
			if len(res) == 0 {
				return fmt.Errorf("foreign check is failed for table '%s'", table)
			}
		}
	}

	return nil
}
