package handlers

import (
	"database/sql"
	"fmt"
	"strings"
)

func InsertPg(conn *sql.DB, table string, columns []string, values [][]any) error {
	var valueStrings []string
	var valueArgs []interface{}
	columnCount := len(columns) // Assuming the number of columns is the same for all rows

	for _, row := range values {
		valuePlaceholders := make([]string, columnCount)
		for i := range row {
			valuePlaceholders[i] = fmt.Sprintf("$%d", len(valueArgs)+1)
			valueArgs = append(valueArgs, row[i])
		}
		valueStrings = append(valueStrings, "("+strings.Join(valuePlaceholders, ", ")+")")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, strings.Join(columns, ","), strings.Join(valueStrings, ", "))

	_, err := conn.Exec(query, valueArgs...)
	return err
}
