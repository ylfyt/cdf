package core

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

func getColumnValuesFromWhere(expr sqlparser.Expr) map[string]string {
	values := make(map[string]string)

	// If the expression is a binary comparison
	if binExpr, ok := expr.(*sqlparser.ComparisonExpr); ok {
		leftCol, ok := binExpr.Left.(*sqlparser.ColName)
		if ok {
			colName := leftCol.Name.String()
			value := sqlparser.String(binExpr.Right)

			// Add column name and value to the map
			values[colName] = value
		}
	}

	// If the expression is a logical AND expression
	if andExpr, ok := expr.(*sqlparser.AndExpr); ok {
		// Recursively process both sides of the AND expression
		leftValues := getColumnValuesFromWhere(andExpr.Left)
		rightValues := getColumnValuesFromWhere(andExpr.Right)

		// Merge the values from both sides
		for col, val := range leftValues {
			values[col] = val
		}
		for col, val := range rightValues {
			values[col] = val
		}
	}

	return values
}

func deleteAction(stmt *sqlparser.Delete) (any, error) {
	where := getColumnValuesFromWhere(stmt.Where.Expr)

	fmt.Printf("Data: %+v\n", where)

	return nil, nil
}
