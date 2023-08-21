package handlers

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
)

func CsInsert(conn *gocql.Session, table string, columns []string, values [][]any) error {
	var valueStrings []string
	var valueArgs []any
	columnCount := len(columns)

	for _, row := range values {
		valuePlaceholders := make([]string, columnCount)
		for i := range row {
			valuePlaceholders[i] = "?"
			valueArgs = append(valueArgs, row[i])
		}
		valueStrings = append(valueStrings, "("+strings.Join(valuePlaceholders, ", ")+")")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, strings.Join(columns, ","), strings.Join(valueStrings, ", "))

	fmt.Println("=== INSERT CS ===")
	fmt.Println(query)
	fmt.Println("*** INSERT CS ***")

	return conn.Query(query, valueArgs...).Exec()
}
