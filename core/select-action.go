package core

import (
	"cdf/models"
	"cdf/utils"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/lib/pq"
	"github.com/xwb1989/sqlparser"
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

func parseArg(arg any) any {
	if reflect.TypeOf(arg).Kind() == reflect.Array {
		return pq.Array(arg)
	}
	return arg
}

func selectPG(conn *sql.DB, table *models.QueryTable, wheres []*models.Cond) ([]map[string]any, error) {
	selects := []string{}
	if len(table.SelectFields) == 0 {
		selects = append(selects, "*")
	}
	for as, field := range table.SelectFields {
		selects = append(selects, fmt.Sprintf("%s AS %s", field, as))
	}

	queryParams := []any{}
	whereQueries := []string{}
	for _, cond := range wheres {
		query := ""
		if cond.Left.Value != nil && cond.Right.Value != nil {
			// queryParams = append(queryParams, parseArg(cond.Left.Value))
			// queryParams = append(queryParams, parseArg(cond.Right.Value))
			// query = fmt.Sprintf("$%d %s $%d", len(queryParams)-1, cond.Op, len(queryParams))
		} else if cond.Left.Value != nil {
			if vals, ok := cond.Left.Value.([]any); ok {
				if len(vals) == 0 {
					query = fmt.Sprintf("%s %s (array[])", cond.Right.Field, cond.Op)
				} else {
					right := ""
					for idx, val := range vals {
						queryParams = append(queryParams, val)
						right += fmt.Sprintf("$%d", len(queryParams))
						if idx != len(vals)-1 {
							right += ","
						}
					}
					query = fmt.Sprintf("%s %s (%s)", cond.Right.Field, cond.Op, right)
				}
			} else {
				queryParams = append(queryParams, cond.Left.Value)
				query = fmt.Sprintf("%s %s $%d", cond.Right.Field, cond.Op, len(queryParams))
			}
		} else if cond.Right.Value != nil {
			if vals, ok := cond.Right.Value.([]any); ok {
				if len(vals) == 0 {
					query = fmt.Sprintf("%s %s (array[])", cond.Left.Field, cond.Op)
				} else {
					right := ""
					for idx, val := range vals {
						queryParams = append(queryParams, val)
						right += fmt.Sprintf("$%d", len(queryParams))
						if idx != len(vals)-1 {
							right += ","
						}
					}
					query = fmt.Sprintf("%s %s (%s)", cond.Left.Field, cond.Op, right)
				}
			} else {
				queryParams = append(queryParams, cond.Right.Value)
				query = fmt.Sprintf("%s %s $%d", cond.Left.Field, cond.Op, len(queryParams))
			}
		} else {
			query = fmt.Sprintf("%s %s %s", cond.Left.Field, cond.Op, cond.Right.Field)
		}
		whereQueries = append(whereQueries, query)
	}

	query := fmt.Sprintf(`
		SELECT
			%s
		FROM
			%s
		%s
	`,
		strings.Join(selects, ","),
		table.Name,
		utils.Ternary(
			len(whereQueries) == 0,
			"",
			fmt.Sprintf("WHERE %s", strings.Join(whereQueries, " AND ")),
		),
	)

	rows, err := conn.Query(query, queryParams...)
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	numOfColumns := len(columns)
	scans := make([]any, numOfColumns)
	scansPtr := make([]any, numOfColumns)

	for i := range scans {
		scansPtr[i] = &scans[i]
	}

	var result []map[string]any
	for rows.Next() {
		err := rows.Scan(scansPtr...)
		if err != nil {
			fmt.Println("Err", err)
			return nil, err
		}
		row := make(map[string]any)
		for i, v := range columns {
			row[v] = scans[i]
		}
		result = append(result, row)
	}

	return result, nil
}

func parseFrom(stmt *sqlparser.Select, fields map[string]map[string]any) (*models.OrderMap[string, *models.QueryTable], error) {
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

func selectAction(stmt *sqlparser.Select) (any, error) {
	fields := parseSelectField(stmt)

	var wheres []*models.Cond
	if stmt.Where != nil {
		wheres = utils.ParseJoinCondition(stmt.Where.Expr)
	}

	queries, err := parseFrom(stmt, fields)
	if err != nil {
		return nil, err
	}

	err = parseDependencyConds(queries)
	if err != nil {
		return nil, err
	}
	err = applyWheres(queries, wheres)
	if err != nil {
		return nil, err
	}

	raw := map[string][]map[string]any{}

	for _, qua := range queries.Keys {
		table := queries.Get(qua)
		// Parse db wheres
		wheres := []*models.Cond{}
		wheres = append(wheres, table.Conds...)

		for _, cond := range table.DepConds {
			if cond.Left.Value != nil || cond.Right.Value != nil {
				wheres = append(wheres, cond)
				continue
			}
			if cond.Left.Qualifier == qua {
				result := raw[cond.Right.Qualifier]
				values := []any{}
				for _, res := range result {
					values = append(values, res[cond.Right.Field])
				}
				wheres = append(wheres, &models.Cond{
					Left: cond.Left,
					Op:   cond.Op,
					Right: models.CondInfo{
						Value: values,
					},
				})
				continue
			}
			if cond.Right.Qualifier == qua {
				result := raw[cond.Left.Qualifier]
				values := []any{}
				for _, res := range result {
					values = append(values, res[cond.Left.Field])
				}
				wheres = append(wheres, &models.Cond{
					Left: models.CondInfo{
						Value: values,
					},
					Op:    cond.Op,
					Right: cond.Right,
				})
				continue
			}
			// TODO: Bukan di table ini
		}

		db := getDb(table.Name)
		if db.Type == "PostgreSQL" {
			res, err := selectPG(db.Conn.(*sql.DB), table, wheres)
			if err != nil {
				return nil, err
			}
			raw[qua] = res
		}
	}

	fmt.Printf("Data: %+v\n", raw)

	// result := [][]map[string]any{}
	// for idx, query := range queries {
	// 	db := getDb(query.Get(query.Keys[0]).Name)
	// 	if db.Type == "PostgreSQL" {
	// 		res, err := selectPG(db.Conn.(*sql.DB), query, queryWheres[idx])
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		result = append(result, res)
	// 	}
	// }
	// nestedJoin(result, deps)

	// fmt.Printf("Result: %+v\n", result)

	// if len(result) > 0 {
	// 	return result[0], nil
	// }

	return nil, nil
}
