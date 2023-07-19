package main

import (
	"cdf/db"
	"cdf/models"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/xwb1989/sqlparser"
)

func insertAction(stmt *sqlparser.Insert) error {
	columnNames := make([]string, 0)
	for _, column := range stmt.Columns {
		columnNames = append(columnNames, column.CompliantName())
	}

	fmt.Println("Column names:", columnNames)

	values, ok := stmt.Rows.(sqlparser.Values)
	if !ok {
		return errors.New("no values found in the insert statement")
	}

	for _, valTuple := range values {
		fmt.Printf("Data: %+v\n", valTuple)

		for _, val := range valTuple {
			switch val := val.(type) {
			case *sqlparser.SQLVal:
				fmt.Println("Value:", string(val.Val))
			case *sqlparser.NullVal:
				fmt.Println("Value: NULL")
			default:
				return errors.New("unsupported value type")
			}
		}
	}

	return nil
}

func main() {
	file, err := os.Open("./schema.json")
	if err != nil {
		panic(err)
	}

	data, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}

	var schema models.Schema
	err = json.Unmarshal(data, &schema)
	if err != nil {
		panic(err)
	}

	db.Start(&schema)

	query := `
	INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com'), (2, 'Budi', NULL)
	`

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		fmt.Printf("Failed to parse query: %v\n", err)
		return
	}

	if insertStmt, ok := stmt.(*sqlparser.Insert); ok {
		insertAction(insertStmt)
		return
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		// Handle SELECT statement
		fmt.Printf("Data: %+v\n", stmt.From[0])

	case *sqlparser.Insert:
		// Handle INSERT statement
		fmt.Println("INSERT statement")

	case *sqlparser.Update:
		// Handle UPDATE statement
		fmt.Println("UPDATE statement")

	case *sqlparser.Delete:
		// Handle DELETE statement
		fmt.Println("DELETE statement")

	default:
		// Handle unsupported statement types
		fmt.Println("Unsupported statement")
	}

	fmt.Printf("Data: %+v\n", stmt)
}
