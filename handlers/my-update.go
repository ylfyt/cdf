package handlers

import (
	"cdf/models"
	"cdf/utils"
	"database/sql"
	"fmt"
	"strings"
)

func (me *HandlerCtx) MyUpdate(conn *sql.DB, table string, wheres []*models.Cond, values map[string]any) (int, error) {
	valueQueries := []string{}
	queryParams := []any{}
	for name, value := range values {
		queryParams = append(queryParams, value)
		valueQueries = append(valueQueries, fmt.Sprintf(`%s = ?`, name))
	}

	whereQueries := buildWhereQuery(wheres, &queryParams, false)

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
