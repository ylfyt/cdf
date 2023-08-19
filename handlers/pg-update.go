package handlers

import (
	"database/sql"
	"fmt"
	"strings"
)

func PgUpdate(conn *sql.DB, table string, wheres map[string]any, values map[string]any) (int, error) {
	valueQueries := []string{}
	args := []any{}
	for name, value := range values {
		args = append(args, value)
		valueQueries = append(valueQueries, fmt.Sprintf(`%s = $%d`, name, len(args)))
	}

	whereQueries := []string{}
	for name, value := range wheres {
		if value == nil {
			whereQueries = append(whereQueries, fmt.Sprintf("%s IS NULL", name))
			continue
		}
		args = append(args, value)
		whereQueries = append(whereQueries, fmt.Sprintf("%s = $%d", name, len(args)))
	}

	query := fmt.Sprintf(`
		UPDATE %s
		SET
			%s
		WHERE
			%s
	`, table, strings.Join(valueQueries, ","), strings.Join(whereQueries, " AND "))

	res, err := conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(affected), nil
}
