package handlers

import (
	"cdf/models"
	"cdf/utils"
	"database/sql"
	"fmt"
	"strings"
)

func (me *HandlerCtx) PgUpdate(conn *sql.DB, table string, wheres []*models.Cond, values map[string]any) (int, error) {
	valueQueries := []string{}
	queryParams := []any{}
	for column, value := range values {
		queryParams = append(queryParams, value)
		cast := ""
		field := me.Fields[column]
		if field == nil {
			return 0, fmt.Errorf("type??")
		}
		if field.Type == "object" || field.Type == "_object" {
			cast = "JSON"
		}
		valueQueries = append(valueQueries, fmt.Sprintf(`%s = $%d%s`, column, len(queryParams), utils.Ternary(cast != "", "::JSON", "")))
	}

	whereQueries := buildWhereQuery(wheres, &queryParams, true)

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

	fmt.Println("=== UPDATE PG ===")
	fmt.Println(query)
	fmt.Println("*** UPDATE PG ***")

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
