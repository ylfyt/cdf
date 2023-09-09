package handlers

import (
	"cdf/models"
	"cdf/utils"
	"database/sql"
	"fmt"
	"strings"
)

func (me *HandlerCtx) PgRead(conn *sql.DB, table *models.QueryTable, wheres []*models.Cond) ([]map[string]any, error) {
	selects := []string{}
	if len(table.SelectFields) == 0 {
		selects = append(selects, "*")
	}
	for as, field := range table.SelectFields {
		selects = append(selects, fmt.Sprintf("%s AS %s", field, as))
	}

	queryParams := []any{}
	whereQueries := buildWhereQuery(wheres, &queryParams, true)

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

	fmt.Println("=== READ PG ===")
	fmt.Println(query)
	fmt.Println("*** READ PG ***")

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
			fmt.Println("Err", err)
			return nil, err
		}
		row := make(map[string]any)
		for i, v := range columns {
			row[v] = scans[i]
		}
		result = append(result, row)
	}

	return result, nil
}
