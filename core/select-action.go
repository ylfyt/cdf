package core

import (
	"cdf/models"
	"cdf/utils"
	"fmt"
	"github.com/xwb1989/sqlparser"
	"reflect"
)

func parseTableExprs(expr sqlparser.TableExpr, tables *models.OrderMap[string, *models.QueryTable], conds []*models.Cond, join string) {
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
			Join:  join,
		}
		tables.Set(as, &tmp)
		return
	}

	if expr, ok := expr.(*sqlparser.JoinTableExpr); ok {
		res := utils.ParseJoinCondition(expr.Condition.On)
		parseTableExprs(expr.LeftExpr, tables, nil, "")
		parseTableExprs(expr.RightExpr, tables, res, expr.Join)
	}
}

func parseDependencyConds(query *models.OrderMap[string, *models.QueryTable]) error {
	for _, key := range query.Keys {
		table := query.Get(key)
		newConds := []*models.Cond{}
		deps := []*models.Cond{}
		for _, cond := range table.Conds {
			left := query.Get(cond.Left.Qualifier)
			right := query.Get(cond.Right.Qualifier)
			if left == nil || right == nil {
				return fmt.Errorf("table not found")
			}
			if cond.Left.Qualifier != cond.Right.Qualifier {
				deps = append(deps, cond)
			} else {
				newConds = append(newConds, cond)
			}
		}
		table.Conds = newConds
		table.DepConds = deps
	}
	return nil
}

func parseFrom(stmt *sqlparser.Select) (*models.OrderMap[string, *models.QueryTable], error) {
	queryTables := models.OrderMap[string, *models.QueryTable]{
		Keys:   []string{},
		Values: make(map[string]*models.QueryTable),
	}
	parseTableExprs(stmt.From[0], &queryTables, nil, "")

	return &queryTables, nil
}

// qualifier -> as -> field
func parseSelectField(stmt *sqlparser.Select) map[string]map[string]any {
	fields := map[string]map[string]any{}
	for _, expr := range stmt.SelectExprs {
		if expr, ok := expr.(*sqlparser.StarExpr); ok {
			qua := expr.TableName.Name.String()
			if fields[qua] == nil {
				fields[qua] = make(map[string]any)
			}
			continue
		}
		if aliased, ok := expr.(*sqlparser.AliasedExpr); ok {
			if val, ok := aliased.Expr.(*sqlparser.SQLVal); ok {
				val, _ := utils.ParseValue(val)
				if fields["=VALUE="] == nil {
					fields["=VALUE="] = make(map[string]any)
				}
				fields["=VALUE="][aliased.As.String()] = val
				continue
			}
			colName := aliased.Expr.(*sqlparser.ColName)
			field := colName.Name.String()
			qua := colName.Qualifier.Name.String()
			as := aliased.As.String()
			if as == "" {
				as = field
			}
			if fields[qua] == nil {
				fields[qua] = make(map[string]any)
			}
			fields[qua][as] = field
			continue
		}
		fmt.Println("???", reflect.TypeOf(expr))
	}

	return fields
}

func getIdxQualifierInQuery(query *models.OrderMap[string, *models.QueryTable], qua string) int {
	for idx, key := range query.Keys {
		if key == qua {
			return idx
		}
	}
	return -1
}

func applyWheres(queries *models.OrderMap[string, *models.QueryTable], wheres []*models.Cond) error {
	for _, cond := range wheres {
		if cond.Left.Value != nil && cond.Right.Value != nil {
			table := queries.Get(queries.Keys[0])
			table.Conds = append(table.Conds, cond)
			continue
		}
		if cond.Left.Value != nil {
			rightTable := queries.Get(cond.Right.Qualifier)
			rightTable.Conds = append(rightTable.Conds, cond)
			continue
		}
		if cond.Right.Value != nil {
			leftTable := queries.Get(cond.Left.Qualifier)
			leftTable.Conds = append(leftTable.Conds, cond)
			continue
		}
		if cond.Left.Qualifier == cond.Right.Qualifier {
			table := queries.Get(cond.Left.Qualifier)
			table.Conds = append(table.Conds, cond)
			continue
		}

		leftIdx := getIdxQualifierInQuery(queries, cond.Left.Qualifier)
		rightIdx := getIdxQualifierInQuery(queries, cond.Right.Qualifier)
		if leftIdx > rightIdx {
			table := queries.Get(queries.Keys[leftIdx])
			table.DepConds = append(table.DepConds, cond)
		} else {
			table := queries.Get(queries.Keys[rightIdx])
			table.DepConds = append(table.DepConds, cond)
		}
	}
	return nil
}

// func shouldJoin(rec1 map[string]any, rec2 map[string]any, conds []*models.Cond) bool {

// }

// func nestedJoin(data [][]map[string]any, deps [][]*models.Cond) []map[string]any {
// 	if len(data) == 0 {
// 		return nil
// 	}
// 	if len(data) == 1 {
// 		return data[0]
// 	}

// 	for len(data) > 1 {
// 		res1 := data[0]
// 		res2 := data[1]
// 		for _, x := range res1 {
// 			for _, y := range res2 {

// 			}
// 		}
// 	}

// 	return nil
// }

func applyDepWheres(depConds []*models.Cond, qua string, wheres *[]*models.Cond, rawValue map[string][]map[string]any) {
	for _, cond := range depConds {
		if cond.Left.Value != nil || cond.Right.Value != nil {
			*wheres = append(*wheres, cond)
			continue
		}
		if cond.Left.Qualifier == qua {
			result := rawValue[cond.Right.Qualifier]
			values := []any{}
			for _, res := range result {
				values = append(values, res[cond.Right.Field])
			}
			*wheres = append(*wheres, &models.Cond{
				Left: cond.Left,
				Op:   "IN",
				Right: models.CondInfo{
					Value: values,
				},
			})
			continue
		}
		if cond.Right.Qualifier == qua {
			result := rawValue[cond.Left.Qualifier]
			values := []any{}
			for _, res := range result {
				values = append(values, res[cond.Left.Field])
			}
			*wheres = append(*wheres, &models.Cond{
				Left: cond.Right,
				Op:   "IN",
				Right: models.CondInfo{
					Value: values,
				},
			})
			continue
		}
		// TODO: Bukan di table ini
	}
}

func buildKey(table *models.QueryTable, qua string, value map[string]any) string {
	key := ""
	for _, cond := range table.DepConds {
		if cond.Left.Qualifier == qua {
			fieldValue := value[cond.Left.Field]
			key += fmt.Sprint(fieldValue) + "_"
		} else if cond.Right.Qualifier == qua {
			fieldValue := value[cond.Right.Field]
			key += fmt.Sprint(fieldValue) + "_"
		}
	}

	return key
}

func (me *Handler) selectAction(stmt *sqlparser.Select) (any, error) {
	fields := parseSelectField(stmt)
	_ = fields

	var wheres []*models.Cond
	if stmt.Where != nil {
		wheres = utils.ParseJoinCondition(stmt.Where.Expr)
	}

	query, err := parseFrom(stmt)
	if err != nil {
		return nil, err
	}

	err = parseDependencyConds(query)
	if err != nil {
		return nil, err
	}
	err = applyWheres(query, wheres)
	if err != nil {
		return nil, err
	}

	raw := map[string][]map[string]any{}

	for _, qua := range query.Keys {
		table := query.Get(qua)
		// Parse db wheres
		wheres := []*models.Cond{}
		wheres = append(wheres, table.Conds...)

		applyDepWheres(table.DepConds, qua, &wheres, raw)

		db := getDb(table.Name)
		if db == nil {
			return nil, fmt.Errorf("db not found for %s", table.Name)
		}
		driver := drivers[db.Type]
		res, err := driver.read(db.Conn, table, wheres)
		if err != nil {
			return nil, err
		}
		raw[qua] = res
	}

	for i := 1; i < len(query.Keys); i++ {
		qua := query.Keys[i]
		table := query.Get(qua)
		result := raw[qua]
		joinMap := map[string][]any{}
		for _, val := range result {
			key := buildKey(table, qua, val)
			if joinMap[key] == nil {
				joinMap[key] = make([]any, 0)
			}
			joinMap[key] = append(joinMap[key], val)
		}
		newVal := []map[string]any{}
		targetQua := query.Keys[i-1]
		target := raw[targetQua]
		for _, val := range target {
			key := buildKey(table, targetQua, val)
			joinValues := joinMap[key]
			val[table.Name] = joinValues
			if table.Join == "join" && len(joinValues) == 0 {
				continue
			}
			newVal = append(newVal, val)
		}
		raw[targetQua] = newVal
	}

	return raw[query.Keys[0]], nil
}
