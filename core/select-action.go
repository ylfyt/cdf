package core

import (
	"cdf/models"
	"cdf/utils"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

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

func parseDependencyConds(queries []*models.OrderMap[string, *models.QueryTable]) [][]*models.Cond {
	deps := [][]*models.Cond{}
	for _, query := range queries {
		dep := []*models.Cond{}
		for _, key := range query.Keys {
			table := query.Get(key)
			newConds := []*models.Cond{}
			for _, cond := range table.Conds {
				left := query.Get(cond.Left.Qualifier)
				right := query.Get(cond.Right.Qualifier)
				if left == nil || right == nil {
					dep = append(dep, cond)
					continue
				}
				newConds = append(newConds, cond)
			}
			table.Conds = newConds
		}
		deps = append(deps, dep)
	}
	return deps
}

func selectPG(conn *sql.DB, tables *models.OrderMap[string, *models.QueryTable], wheres []*models.Cond) ([]map[string]any, error) {
	froms := []string{}
	selects := []string{}
	for idx, qualifier := range tables.Keys {
		table := tables.Get(qualifier)
		conds := []string{}
		for _, cond := range table.Conds {
			left := ""
			if cond.Left.Value != nil {
				left += fmt.Sprint(cond.Left.Value)
			} else {
				if cond.Left.Qualifier != "" {
					left += cond.Left.Qualifier + "." + cond.Left.Field
				} else {
					left += cond.Left.Field
				}
			}

			right := ""
			if cond.Right.Value != nil {
				right += fmt.Sprint(cond.Right.Value)
			} else {
				if cond.Right.Qualifier != "" {
					right += cond.Right.Qualifier + "." + cond.Right.Field
				} else {
					right += cond.Right.Field
				}
			}

			condStr := fmt.Sprintf("%s %s %s", left, cond.Op, right)
			conds = append(conds, condStr)
		}

		from := ""
		if table.Join == "" || idx == 0 {
			if qualifier == table.Name {
				from = qualifier
			} else {
				from = fmt.Sprintf("%s %s", table.Name, qualifier)
			}
		} else {
			if qualifier == table.Name {
				from = fmt.Sprintf("%s %s %s", table.Join, table.Name, utils.Ternary(len(conds) == 0, "", "ON "+strings.Join(conds, " AND ")))
			} else {
				from = fmt.Sprintf("%s %s %s %s", table.Join, table.Name, qualifier, utils.Ternary(len(conds) == 0, "", "ON "+strings.Join(conds, " AND ")))
			}
		}
		if len(table.SelectFields) == 0 {
			selects = append(selects, fmt.Sprintf("%s.*", qualifier))
		}
		for as, field := range table.SelectFields {
			selects = append(selects, fmt.Sprintf("%s.%s AS %s", qualifier, field, as))
		}

		froms = append(froms, from)
	}

	queryParams := []any{}
	whereQueries := []string{}
	for _, cond := range wheres {
		left := ""
		if cond.Left.Value != nil {
			queryParams = append(queryParams, cond.Left.Value)
			left = fmt.Sprintf("$%d", len(queryParams))
		} else {
			if cond.Left.Qualifier == "" {
				left = cond.Left.Field
			} else {
				left = fmt.Sprintf("%s.%s", cond.Left.Qualifier, cond.Left.Field)
			}
		}

		right := ""
		if cond.Right.Value != nil {
			queryParams = append(queryParams, cond.Right.Value)
			right = fmt.Sprintf("$%d", len(queryParams))
		} else {
			if cond.Right.Qualifier == "" {
				right = cond.Right.Field
			} else {
				right = fmt.Sprintf("%s.%s", cond.Right.Qualifier, cond.Right.Field)
			}
		}

		query := fmt.Sprintf("%s %s %s", left, cond.Op, right)
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
		strings.Join(froms, " "),
		utils.Ternary(
			len(whereQueries) == 0,
			"",
			fmt.Sprintf("WHERE %s", strings.Join(whereQueries, " AND ")),
		),
	)

	fmt.Println("QUERY:", query)

	// rows, err := conn.Query(query, queryParams...)
	// if err != nil {
	// 	fmt.Println("Err", err)
	// 	return nil, err
	// }
	// columns, err := rows.Columns()
	// if err != nil {
	// 	fmt.Println("Err", err)
	// 	return nil, err
	// }

	// numOfColumns := len(columns)
	// scans := make([]any, numOfColumns)
	// scansPtr := make([]any, numOfColumns)

	// for i := range scans {
	// 	scansPtr[i] = &scans[i]
	// }

	// var result []map[string]any
	// for rows.Next() {
	// 	err := rows.Scan(scansPtr...)
	// 	if err != nil {
	// 		fmt.Println("Err", err)
	// 		return nil, err
	// 	}
	// 	row := make(map[string]any)
	// 	for i, v := range columns {
	// 		row[v] = scans[i]
	// 	}
	// 	result = append(result, row)
	// }

	return nil, nil
}

func parseFrom(stmt *sqlparser.Select, fields map[string]map[string]any) ([]*models.OrderMap[string, *models.QueryTable], error) {
	queryTables := models.OrderMap[string, *models.QueryTable]{
		Keys:   []string{},
		Values: make(map[string]*models.QueryTable),
	}
	parseTableExprs(stmt.From[0], &queryTables, nil, "")

	queries := []*models.OrderMap[string, *models.QueryTable]{}
	var tmpOrderMap *models.OrderMap[string, *models.QueryTable] = nil
	var lastDb *database = nil

	for qua := range fields {
		if qua == "" {
			continue
		}
		table := queryTables.Get(qua)
		if table == nil {
			return nil, fmt.Errorf("qualifier %s is not found", qua)
		}
	}

	for _, qua := range queryTables.Keys {
		queryTable := queryTables.Get(qua)
		key := qua
		if qua == queryTable.Name {
			key = ""
		}
		queryTable.SelectFields = fields[key]

		db := getDb(queryTable.Name)
		if db == nil {
			return nil, fmt.Errorf("db not found for %s", queryTable.Name)
		}

		if tmpOrderMap == nil {
			tmpOrderMap = &models.OrderMap[string, *models.QueryTable]{
				Keys:   make([]string, 0),
				Values: make(map[string]*models.QueryTable),
			}
			lastDb = db
			tmpOrderMap.Set(qua, queryTable)
			continue
		}

		if db != lastDb {
			queries = append(queries, tmpOrderMap)
			tmpOrderMap = nil
			lastDb = db
			tmpOrderMap = &models.OrderMap[string, *models.QueryTable]{
				Keys:   make([]string, 0),
				Values: make(map[string]*models.QueryTable),
			}
			tmpOrderMap.Set(qua, queryTable)
			continue
		}
		tmpOrderMap.Set(qua, queryTable)
	}
	queries = append(queries, tmpOrderMap)
	return queries, nil
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

func getIdxQualifierInQuery(queries []*models.OrderMap[string, *models.QueryTable], qua string) int {
	for i, query := range queries {
		_, exist := query.GetExist(qua)
		if exist {
			return i
		}
	}
	return -1
}

func applyWheres(queries []*models.OrderMap[string, *models.QueryTable], wheres []*models.Cond) ([][]*models.Cond, []*models.Cond) {
	queryWheres := make([][]*models.Cond, len(queries))
	deps := []*models.Cond{}
	for _, cond := range wheres {
		if cond.Left.Value != nil {
			idx := getIdxQualifierInQuery(queries, cond.Right.Qualifier)
			if idx != -1 {
				queryWheres[idx] = append(queryWheres[idx], cond)
			} else {
				deps = append(deps, cond)
			}
			continue
		}
		if cond.Right.Value != nil {
			idx := getIdxQualifierInQuery(queries, cond.Left.Qualifier)
			if idx != -1 {
				queryWheres[idx] = append(queryWheres[idx], cond)
			} else {
				deps = append(deps, cond)
			}
			continue
		}

		leftIdx := getIdxQualifierInQuery(queries, cond.Left.Qualifier)
		rightIdx := getIdxQualifierInQuery(queries, cond.Right.Qualifier)
		if leftIdx == -1 || rightIdx == -1 || (leftIdx != rightIdx) {
			deps = append(deps, cond)
			continue
		}
		queryWheres[leftIdx] = append(queryWheres[leftIdx], cond)
	}
	return queryWheres, deps
}

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

	deps := parseDependencyConds(queries)
	_ = deps

	queryWheres, depWheres := applyWheres(queries, wheres)
	_ = depWheres

	result := [][]map[string]any{}
	for idx, query := range queries {
		db := getDb(query.Get(query.Keys[0]).Name)
		if db.Type == "PostgreSQL" {
			res, err := selectPG(db.Conn.(*sql.DB), query, queryWheres[idx])
			if err != nil {
				return nil, err
			}
			result = append(result, res)
		}
	}
	fmt.Printf("Result: %+v\n", result)

	return nil, nil
}
