package handlers

import (
	"cdf/models"
	"cdf/utils"
	"database/sql"
	"fmt"
	"strings"
)

func MyUpdate(conn *sql.DB, table string, wheres []*models.Cond, values map[string]any) (int, error) {
	valueQueries := []string{}
	queryParams := []any{}
	for name, value := range values {
		queryParams = append(queryParams, value)
		valueQueries = append(valueQueries, fmt.Sprintf(`%s = ?`, name))
	}

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
			query = fmt.Sprintf("%s %s %s", cond.Left.Field, cond.Op, cond.Right.Field)
		}
		whereQueries = append(whereQueries, query)
	}

	query := fmt.Sprintf(`
		UPDATE %s
		SET
			%s
		%s
	`, table, strings.Join(valueQueries, ","), utils.Ternary(
		len(whereQueries) == 0,
		"",
		fmt.Sprintf("WHERE %s", strings.Join(whereQueries, " AND ")),
	))

	fmt.Println("=== UPDATE MY ===")
	fmt.Println(query)
	fmt.Println("*** UPDATE MY ***")

	res, err := conn.Exec(query, queryParams...)
	if err != nil {
		return 0, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(affected), nil
}
