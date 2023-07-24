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

func selectPG(conn *sql.DB, query *models.OrderMap[string, *models.QueryTable]) {
	froms := []string{}
	for _, qualifier := range query.Keys {
		table := query.Get(qualifier)
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

		froms = append(froms, from)
	}

	fmt.Printf("Data: %+v\n", froms)
}

func selectAction(stmt *sqlparser.Select) (any, error) {
	queryTables := models.OrderMap[string, *models.QueryTable]{
		Keys:   []string{},
		Values: make(map[string]*models.QueryTable),
	}
	parseTableExprs(stmt.From[0], &queryTables, nil, "")

	queries := []*models.OrderMap[string, *models.QueryTable]{}
	var tmpOrderMap *models.OrderMap[string, *models.QueryTable] = nil
	var lastDb *database = nil

	for _, key := range queryTables.Keys {
		queryTable := queryTables.Get(key)
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
			tmpOrderMap.Set(key, queryTable)
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
			tmpOrderMap.Set(key, queryTable)
			continue
		}
		tmpOrderMap.Set(key, queryTable)
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

	wheres := map[string]any{}
	if stmt.Where != nil {
		wheres = getColumnValuesFromWhere(stmt.Where.Expr)
	}

	fmt.Printf("Where %+v\n", wheres)

	type SelectField struct {
		Qualifier string
		Field     string
		As        string
		Val       any
	}

	fields := []SelectField{}
	for _, expr := range stmt.SelectExprs {
		if expr, ok := expr.(*sqlparser.StarExpr); ok {
			field := SelectField{
				Qualifier: expr.TableName.Name.String(),
				Field:     "",
			}
			fields = append(fields, field)
			continue
		}
		if aliased, ok := expr.(*sqlparser.AliasedExpr); ok {
			if val, ok := aliased.Expr.(*sqlparser.SQLVal); ok {
				val, _ := utils.ParseValue(val)
				field := SelectField{
					Qualifier: "",
					Field:     "SQLVal",
					As:        aliased.As.String(),
					Val:       val,
				}
				fields = append(fields, field)
				continue
			}

			colName := aliased.Expr.(*sqlparser.ColName)
			field := SelectField{
				Qualifier: colName.Qualifier.Name.String(),
				Field:     colName.Name.String(),
				As:        aliased.As.String(),
			}
			fields = append(fields, field)
			continue
		}
		fmt.Println("???", reflect.TypeOf(expr))
	}

	fmt.Printf("Fields: %+v\n", fields)

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
