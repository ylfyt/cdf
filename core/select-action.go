package core

import (
	"fmt"
	"reflect"

	"github.com/xwb1989/sqlparser"
)

type OrderMap[T comparable] struct {
	Keys   []T
	Values map[T]any
}

func (me *OrderMap[T]) Set(key T, val any) {
	if _, exist := me.Values[key]; exist {
		me.Values[key] = val
		return
	}
	me.Values[key] = val
	me.Keys = append(me.Keys, key)
}

func (me *OrderMap[T]) Get(key T) any {
	return me.Values[key]
}

func (me *OrderMap[T]) GetExist(key T) (any, bool) {
	val, exist := me.Values[key]
	return val, exist
}

func parseTableExprs(expr sqlparser.TableExpr, tables *OrderMap[string]) error {
	if expr, ok := expr.(*sqlparser.AliasedTableExpr); ok {
		as := expr.As.String()
		table := expr.Expr.(sqlparser.TableName)
		tableName := table.Name.String()
		if as == "" {
			as = tableName
		}
		tables.Set(as, tableName)
		return nil
	}

	if expr, ok := expr.(*sqlparser.JoinTableExpr); ok {
		parseTableExprs(expr.LeftExpr, tables)
		parseTableExprs(expr.RightExpr, tables)
	}

	return nil
}

func selectAction(stmt *sqlparser.Select) (any, error) {
	for _, from := range stmt.From {
		tables := OrderMap[string]{
			Keys:   []string{},
			Values: map[string]any{},
		}
		parseTableExprs(from, &tables)
	}

	type ColName struct{
		
	}

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
