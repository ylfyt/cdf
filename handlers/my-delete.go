package handlers

import (
	"cdf/models"
	"cdf/utils"
	"database/sql"
	"fmt"
	"strings"
)

func (me *HandlerCtx) MyDelete(conn *sql.DB, table string, wheres []*models.Cond) (int, error) {
	queryParams := []any{}
	whereQueries := buildWhereQuery(wheres, &queryParams, false)

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
