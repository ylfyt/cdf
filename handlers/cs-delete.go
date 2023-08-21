package handlers

import (
	"cdf/models"
	"cdf/utils"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
)

func CsDelete(conn *gocql.Session, table string, wheres []*models.Cond) (int, error) {
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

	fmt.Println("=== DELETE CS ===")
	fmt.Println(query)
	fmt.Println("*** DELETE CS ***")

	err := conn.Query(query, queryParams...).Exec()
	if err != nil {
		return 0, err
	}

	return 0, nil
}
