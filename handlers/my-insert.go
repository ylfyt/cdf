package handlers

import (
	"database/sql"
	"fmt"
	"strings"
)

func (me *HandlerCtx) MyInsert(conn *sql.DB, table string, columns []string, values [][]any) error {
	var valueStrings []string
	var valueArgs []interface{}
	columnCount := len(columns) // Assuming the number of columns is the same for all rows

	for _, row := range values {
		valuePlaceholders := make([]string, columnCount)
		for i := range row {
			valuePlaceholders[i] = "?"
			valueArgs = append(valueArgs, row[i])
		}
		valueStrings = append(valueStrings, "("+strings.Join(valuePlaceholders, ", ")+")")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, strings.Join(columns, ","), strings.Join(valueStrings, ", "))

	fmt.Println("=== INSERT MY ===")
	fmt.Println(query)
	fmt.Println("*** INSERT MY ***")

	_, err := conn.Exec(query, valueArgs...)
	return err
}
