package core

import (
	"cdf/models"
	"cdf/utils"
	"fmt"
	"reflect"

	"github.com/xwb1989/sqlparser"
)

func parseTableExprs(expr sqlparser.TableExpr, tables *models.OrderMap[string, *models.QueryTable], conds []*models.Cond) error {
	if expr, ok := expr.(*sqlparser.AliasedTableExpr); ok {
		as := expr.As.String()
		table := expr.Expr.(sqlparser.TableName)
		tableName := table.Name.String()
		if as == "" {
			as = tableName
		}

		tmp := models.QueryTable{
			Name:  tableName,
			Conds: conds,
		}
		tables.Set(as, &tmp)
		return nil
	}

	if expr, ok := expr.(*sqlparser.JoinTableExpr); ok {
		res := utils.ParseJoinCondition(expr.Condition.On)
		parseTableExprs(expr.LeftExpr, tables, nil)
		parseTableExprs(expr.RightExpr, tables, res)
	}

	return nil
}

func selectAction(stmt *sqlparser.Select) (any, error) {
	tables := models.OrderMap[string, *models.QueryTable]{
		Keys:   []string{},
		Values: make(map[string]*models.QueryTable),
	}
	parseTableExprs(stmt.From[0], &tables, nil)

	fmt.Printf("Data: %+v\n", tables)

	return nil, nil

	for _, expr := range stmt.SelectExprs {
		if expr, ok := expr.(*sqlparser.StarExpr); ok {
			fmt.Printf("Star %+v\n", expr)
			continue
		}
		if aliased, ok := expr.(*sqlparser.AliasedExpr); ok {
			if _, ok := aliased.Expr.(*sqlparser.SQLVal); ok {
				// TODO: SQLVal
				continue
			}
			colName := aliased.Expr.(*sqlparser.ColName)
			fmt.Printf("Data: %+v\n", colName)

			as := aliased.As.String()
			if as == "" {

			}
			continue
		}
		fmt.Println("???", reflect.TypeOf(expr))
	}

	return nil, nil

	// mainFields := map[string]string{}
	// fields := map[string]map[string]string{}
	for _, selectExpr := range stmt.SelectExprs {
		if expr, ok := selectExpr.(*sqlparser.AliasedExpr); ok {
			fmt.Printf("Data: %+v\n", expr)

			continue
		}

		if expr, ok := selectExpr.(*sqlparser.StarExpr); ok {
			fmt.Printf("Data: %+v\n", expr)
			continue
		}

		fmt.Println("???", selectExpr)
		return nil, fmt.Errorf("unsupported expr %+v", selectExpr)
	}

	return nil, nil
}
