package handlers

import (
	"cdf/models"
	"cdf/utils"
	"database/sql"
	"fmt"
	"strings"
)

func MyDelete(conn *sql.DB, table string, wheres []*models.Cond) (int, error) {
	queryParams := []any{}
	whereQueries := []string{}
	for _, cond := range wheres {
		query := ""
		if cond.Left.Value != nil && cond.Right.Value != nil {
			// TODO:
		} else if cond.Left.Value != nil {
			// TODO: Check if deps or not
			if vals, ok := cond.Left.Value.([]any); ok {
				if len(vals) == 0 {
					query = "FALSE"
				} else {
					right := ""
					for idx, val := range vals {
						queryParams = append(queryParams, val)
						right += "?"
						if idx != len(vals)-1 {
							right += ","
						}
					}
					query = fmt.Sprintf("%s IN (%s)", cond.Right.Field, right)
				}
			} else {
				queryParams = append(queryParams, cond.Left.Value)
				query = fmt.Sprintf("%s %s ?", cond.Right.Field, cond.Op)
			}
		} else if cond.Right.Value != nil {
			if vals, ok := cond.Right.Value.([]any); ok {
				if len(vals) == 0 {
					query = "FALSE"
				} else {
					right := ""
					for idx, val := range vals {
						queryParams = append(queryParams, val)
						right += "?"
						if idx != len(vals)-1 {
							right += ","
						}
					}
					query = fmt.Sprintf("%s IN (%s)", cond.Left.Field, right)
				}
			} else {
				queryParams = append(queryParams, cond.Right.Value)
				query = fmt.Sprintf("%s %s ?", cond.Left.Field, cond.Op)
			}
		} else {
			if cond.Right.Field == "" {
				queryParams = append(queryParams, nil)
				query = fmt.Sprintf("%s %s ?", cond.Left.Field, cond.Op)
			} else if cond.Left.Field == "" {
				queryParams = append(queryParams, nil)
				query = fmt.Sprintf("? %s %s", cond.Op, cond.Right.Field)
			} else {
				query = fmt.Sprintf("%s %s %s", cond.Left.Field, cond.Op, cond.Right.Field)
			}
		}
		whereQueries = append(whereQueries, query)
	}

	query := fmt.Sprintf(`
		DELETE FROM %s
		%s
		
	`, table, utils.Ternary(
		len(whereQueries) == 0,
		"",
		fmt.Sprintf("WHERE %s", strings.Join(whereQueries, " AND ")),
	))

	fmt.Println("=== DELETE MY ===")
	fmt.Println(query)
	fmt.Println("*** DELETE MY ***")

	res, err := conn.Exec(query, queryParams...)
	if err != nil {
		return 0, err
	}

	affectedRows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(affectedRows), nil
}
