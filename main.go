package main

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
)

func main() {
	query := "SELECT * FROM users WHERE age > 18"

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		fmt.Printf("Failed to parse query: %v\n", err)
		return
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		// Handle SELECT statement
		fmt.Println("Parsed SELECT statement")

		// Access table name
		tableName := stmt.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()
		fmt.Printf("Table name: %s\n", tableName)

		// Access WHERE clause
		whereClause := stmt.Where.Expr
		fmt.Printf("WHERE clause: %v\n", whereClause)

		// Access SELECT expressions
		for _, expr := range stmt.SelectExprs {
			fmt.Printf("SELECT expression: %v\n", expr)
		}

		// ... handle other parts of the SELECT statement

	default:
		// Handle other types of statements
		fmt.Printf("Unsupported statement type: %T\n", stmt)
	}
}
