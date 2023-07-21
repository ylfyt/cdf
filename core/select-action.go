package core

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

func traverseFromClause(node sqlparser.TableExprs) []string {
	fmt.Printf("Data: %+v\n", node[0])
	return nil
}

func traverseTableExpr(expr sqlparser.SimpleTableExpr) string {
	fmt.Printf("Data: %+v\n", expr)
	return ""
}
func selectAction(stmt *sqlparser.Select) (any, error) {
	// Initialize a slice to store the table names in order
	var tableNames []string

	// Traverse the FROM clause to extract table names
	if stmt.From != nil {
		tableNames = traverseFromClause(stmt.From)
	}

	// Print the list of table names in order
	fmt.Println("Table list:")
	for _, tableName := range tableNames {
		fmt.Println(tableName)
	}

	return nil, nil
}
