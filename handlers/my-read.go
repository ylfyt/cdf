package handlers

import (
	"cdf/models"
	"cdf/utils"
	"database/sql"
	"fmt"
	"strings"
)

func MyRead(conn *sql.DB, table *models.QueryTable, wheres []*models.Cond) ([]map[string]any, error) {
	selects := []string{}
	if len(table.SelectFields) == 0 {
		selects = append(selects, "*")
	}
	for as, field := range table.SelectFields {
		selects = append(selects, fmt.Sprintf("%s AS %s", field, as))
	}

	queryParams := []any{}
	whereQueries := []string{}
	for _, cond := range wheres {
		query := ""
		if cond.Left.Value != nil && cond.Right.Value != nil {
			// queryParams = append(queryParams, parseArg(cond.Left.Value))
			// queryParams = append(queryParams, parseArg(cond.Right.Value))
			// query = fmt.Sprintf("$%d %s $%d", len(queryParams)-1, cond.Op, len(queryParams))
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
		SELECT
			%s
		FROM
			%s
		%s
	`,
		strings.Join(selects, ","),
		table.Name,
		utils.Ternary(
			len(whereQueries) == 0,
			"",
			fmt.Sprintf("WHERE %s", strings.Join(whereQueries, " AND ")),
		),
	)

	fmt.Println("=== READ MY ===")
	fmt.Println(query)
	fmt.Println("*** READ MY ***")

	rows, err := conn.Query(query, queryParams...)
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	numOfColumns := len(columns)
	scans := make([]any, numOfColumns)
	scansPtr := make([]any, numOfColumns)

	for i := range scans {
		scansPtr[i] = &scans[i]
	}

	var result []map[string]any
	for rows.Next() {
		err := rows.Scan(scansPtr...)
		if err != nil {
			return nil, err
		}
		row := make(map[string]any)
		for i, v := range columns {
			if val, ok := scans[i].([]byte); ok {
				row[v] = string(val)
			} else {
				row[v] = scans[i]
			}
		}
		result = append(result, row)
	}

	return result, nil
}
