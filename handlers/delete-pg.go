package handlers

import (
	"database/sql"
	"fmt"
	"strings"
)

func DeletePg(conn *sql.DB, table string, wheres map[string]any) (int, error) {
	whereExpr := []string{}
	args := []any{}
	for name, val := range wheres {
		whereExpr = append(whereExpr, fmt.Sprintf("%s = $%d", name, len(args)+1))
		args = append(args, val)
	}

	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE
			%s
	`, table, strings.Join(whereExpr, " AND "))

	res, err := conn.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	affectedRows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(affectedRows), nil
}
