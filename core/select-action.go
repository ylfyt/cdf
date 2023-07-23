package core

import (
	"cdf/utils"
	"fmt"
	"reflect"

	"github.com/xwb1989/sqlparser"
)

type Table struct {
	Cond map[string]any
	Name string
}

type OrderMap[T comparable, R any] struct {
	Keys   []T
	Values map[T]R
}

func (me *OrderMap[T, R]) Set(key T, val R) {
	if _, exist := me.Values[key]; exist {
		me.Values[key] = val
		return
	}
	me.Values[key] = val
	me.Keys = append(me.Keys, key)
}

func (me *OrderMap[T, R]) Get(key T) R {
	return me.Values[key]
}

func (me *OrderMap[T, R]) GetExist(key T) (R, bool) {
	val, exist := me.Values[key]
	return val, exist
}

func parseTableExprs(expr sqlparser.TableExpr, tables *OrderMap[string, *Table], cond string) error {
	if expr, ok := expr.(*sqlparser.AliasedTableExpr); ok {
		as := expr.As.String()
		table := expr.Expr.(sqlparser.TableName)
		tableName := table.Name.String()
		if as == "" {
			as = tableName
		}
		
		tmp := Table{
			Name: tableName,
			Cond: nil,	
		}
		tables.Set(as, &tmp)
		return nil
	}

	if expr, ok := expr.(*sqlparser.JoinTableExpr); ok {
		res := utils.ParseJoinCondition(expr.Condition.On)
		fmt.Printf("Data: %+v\n", res)
		parseTableExprs(expr.LeftExpr, tables, "")
		parseTableExprs(expr.RightExpr, tables, sqlparser.String(expr.Condition))
	}

	return nil
}

func selectAction(stmt *sqlparser.Select) (any, error) {
	tables := OrderMap[string, *Table]{
		Keys:   []string{},
		Values: make(map[string]*Table),
	}
	parseTableExprs(stmt.From[0], &tables, "")

	fmt.Printf("Data: %+v\n", tables.Get("p"))


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
