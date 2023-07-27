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

func parseTableExprs(expr sqlparser.TableExpr, tables *models.OrderMap[string, *models.QueryTable], conds []*models.Cond, join string) error {
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
		return nil
	}

	if expr, ok := expr.(*sqlparser.JoinTableExpr); ok {
		res := utils.ParseJoinCondition(expr.Condition.On)
		parseTableExprs(expr.LeftExpr, tables, nil, "")
		parseTableExprs(expr.RightExpr, tables, res, expr.Join)
	}

	return nil
}

func selectPG(conn *sql.DB, tables *models.OrderMap[string, *models.QueryTable]) {
	froms := []string{}
	selects := []string{}
	for _, qualifier := range tables.Keys {
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
		if table.Join == "" {
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

		if table.SelectFields != nil && len(table.SelectFields) == 0 {
			selects = append(selects, fmt.Sprintf("%s.*", qualifier))
		}
		for as, field := range table.SelectFields {
			selects = append(selects, fmt.Sprintf("%s.%s AS %s", qualifier, field, as))
		}

		froms = append(froms, from)
	}

	query := fmt.Sprintf(`
		SELECT
			%s
		FROM
			%s
	`, strings.Join(selects, ","), strings.Join(froms, " "))

	fmt.Println("QUERY:", query)

	rows, err := conn.Query(query)
	if err != nil {
		fmt.Println("Err", err)
		return
	}
	columns, err := rows.Columns()
	if err != nil {
		fmt.Println("Err", err)
		return
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
			return
		}
		row := make(map[string]any)
		for i, v := range columns {
			row[v] = scans[i]
		}
		result = append(result, row)
	}

	fmt.Println("Data:", result)
}

func selectAction(stmt *sqlparser.Select) (any, error) {
	type SelectField struct {
		Qualifier string
		Field     string
		As        string
		Val       any
	}

	// qualifier -> as -> field
	fields := map[string]map[string]any{}
	// fields := []SelectField{}
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

	fmt.Printf("Fields: %+v\n", fields)

	wheres := map[string]any{}
	if stmt.Where != nil {
		wheres = getColumnValuesFromWhere(stmt.Where.Expr)
	}

	fmt.Printf("Where %+v\n", wheres)

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

	for _, query := range queries {
		// fmt.Printf("Qu: %+v\n", query)
		db := getDb(query.Get(query.Keys[0]).Name)
		if db.Type == "PostgreSQL" {
			selectPG(db.Conn.(*sql.DB), query)
		}
	}

	return nil, nil
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
