package utils

import (
	"cdf/models"

	"github.com/xwb1989/sqlparser"
)

func parseComparison(expr *sqlparser.ComparisonExpr) models.Cond {
	// TODO: Check op
	var leftCond models.CondInfo
	if left, ok := expr.Left.(*sqlparser.ColName); ok {
		qua := left.Qualifier.Name.String()
		field := left.Name.String()
		leftCond = models.CondInfo{
			Qualifier: qua,
			Field:     field,
		}
	} else {
		val, _ := ParseValue(expr.Left.(*sqlparser.SQLVal))
		leftCond = models.CondInfo{
			Value: val,
		}
	}

	var rightCond models.CondInfo
	if col, ok := expr.Left.(*sqlparser.ColName); ok {
		qua := col.Qualifier.Name.String()
		field := col.Name.String()
		rightCond = models.CondInfo{
			Qualifier: qua,
			Field:     field,
		}
	} else {
		val, _ := ParseValue(expr.Left.(*sqlparser.SQLVal))
		rightCond = models.CondInfo{
			Value: val,
		}
	}

	return models.Cond{
		Left:  leftCond,
		Right: rightCond,
		Op:    expr.Operator,
	}
}

func ParseJoinCondition(expr sqlparser.Expr) []*models.Cond {
	values := []*models.Cond{}

	// If the expression is a binary comparison
	if binExpr, ok := expr.(*sqlparser.ComparisonExpr); ok {
		cond := parseComparison(binExpr)
		values = append(values, &cond)
	}

	// If the expression is a logical AND expression
	if andExpr, ok := expr.(*sqlparser.AndExpr); ok {
		// Recursively process both sides of the AND expression
		leftValues := ParseJoinCondition(andExpr.Left)
		rightValues := ParseJoinCondition(andExpr.Right)

		// Merge the values from both sides
		values = append(values, leftValues...)
		values = append(values, rightValues...)
	}

	return values
}
