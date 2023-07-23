package utils

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

func ParseJoinCondition(expr sqlparser.Expr) map[string]any {
	type CondInfo struct {
		Qualifier string
		Field     string
		Value any
	}

	type Cond struct {
		Left  CondInfo
		Right CondInfo
	}

	values := make(map[string]any)

	// If the expression is a binary comparison
	if binExpr, ok := expr.(*sqlparser.ComparisonExpr); ok {
		// TODO: Check op
		var leftCond CondInfo
		if left, ok := binExpr.Left.(*sqlparser.ColName); ok {
			qua := left.Qualifier.Name.String()
			field := left.Name.String()
			leftCond = CondInfo{
				Qualifier: qua,
				Field: field,
			}
		}

		leftCol, ok := binExpr.Left.(*sqlparser.ColName)
		if ok {
			colName := leftCol.Name.String()
			val, _ := ParseValue(binExpr.Right)

			// Add column name and value to the map
			values[colName] = val
		}
	}

	// If the expression is a logical AND expression
	if andExpr, ok := expr.(*sqlparser.AndExpr); ok {
		// Recursively process both sides of the AND expression
		leftValues := ParseJoinCondition(andExpr.Left)
		rightValues := ParseJoinCondition(andExpr.Right)

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
