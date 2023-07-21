package main

import (
	"cdf/core"
	"cdf/models"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/xwb1989/sqlparser"
)

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

	core.Start(&schema)

	query := `
		INSERT INTO orders (name, email) VALUES ('John Doe', 'john@example.com'), ('Budi', 'budi@gmail.com')
	`

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		fmt.Printf("Failed to parse query: %v\n", err)
		return
	}

	err = core.Execute(stmt)
	if err != nil {
		fmt.Println("Err", err)
	}

	// switch stmt := stmt.(type) {
	// case *sqlparser.Select:
	// 	// Handle SELECT statement
	// 	fmt.Printf("Data: %+v\n", stmt.From[0])

	// case *sqlparser.Insert:
	// 	// Handle INSERT statement
	// 	fmt.Println("INSERT statement")

	// case *sqlparser.Update:
	// 	// Handle UPDATE statement
	// 	fmt.Println("UPDATE statement")

	// case *sqlparser.Delete:
	// 	// Handle DELETE statement
	// 	fmt.Println("DELETE statement")

	// default:
	// 	// Handle unsupported statement types
	// 	fmt.Println("Unsupported statement")
	// }

	// fmt.Printf("Data: %+v\n", stmt)
}
