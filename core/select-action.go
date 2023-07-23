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
	// fields := map[string]bool{}
	for _, selectExpr := range stmt.SelectExprs {
		if expr, ok := selectExpr.(*sqlparser.StarExpr); ok {
			fmt.Printf("Data: %+v\n", expr)
			continue
		}

		if expr, ok := selectExpr.(*sqlparser.AliasedExpr); ok {
			fmt.Printf("Data: %+v\n", expr)
			continue
		}
		fmt.Println("???", selectExpr)
		return nil, fmt.Errorf("unsupported expr %+v", selectExpr)
	}

	return nil, nil
}
