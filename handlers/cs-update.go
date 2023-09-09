package handlers

import (
	"cdf/models"
	"cdf/utils"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
)

func (me *HandlerCtx) CsUpdate(conn *gocql.Session, table string, wheres []*models.Cond, values map[string]any) (int, error) {
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

	err := conn.Query(query, queryParams...).Exec()
	if err != nil {
		return 0, err
	}

	return 0, nil
}
