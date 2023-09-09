package core

import (
	"cdf/models"
	"cdf/utils"
	"fmt"
	"reflect"
	"strings"

	"github.com/xwb1989/sqlparser"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (me *Handler) selectAction(stmt *sqlparser.Select) (any, error) {
	query, err := parseFrom(stmt)
	if err != nil {
		return nil, err
	}
	if len(query.Keys) == 0 {
		return nil, fmt.Errorf("from clause cannot empty")
	}

	fields, err := parseSelectField(stmt, query)
	if err != nil {
		return nil, err
	}

	tables := map[string]bool{}
	for _, field := range fields {
		tables[field.Table] = true
	}

	err = me.validateAuth(tables, query, false, nil)
	if err != nil {
		return nil, err
	}

	var wheres []*models.Cond
	if stmt.Where != nil {
		wheres = utils.ParseJoinCondition(stmt.Where.Expr)
	}
	for _, where := range wheres {
		if len(query.Keys) > 1 {
			if where.Left.Field != "" && where.Left.Qualifier == "" {
				return nil, fmt.Errorf("qualifier cannot empty when tables > 1")
			}
			if where.Right.Field != "" && where.Right.Qualifier == "" {
				return nil, fmt.Errorf("qualifier cannot empty when tables > 1")
			}
		} else {
			if where.Left.Qualifier == "" {
				where.Left.Qualifier = query.Keys[0]
			}
			if where.Right.Qualifier == "" {
				where.Right.Qualifier = query.Keys[0]
			}
		}
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

	// PENDING RULES
	err = me.validateAuth(tables, query, true, raw)
	if err != nil {
		return nil, err
	}

	res := processJoin(query, raw)
	final := generateFinalValue(res, fields, query)
	return final, nil
}

func (me *Handler) validateAuth(tables map[string]bool, query *models.OrderMap[string, *models.QueryTable], validateData bool, raw map[string][]map[string]any) error {
	for table := range tables {
		db := getDb(table)
		if db == nil {
			return fmt.Errorf("table %s is not found", table)
		}
		rules := readAuthRules[db.Name]
		err := me.validateRules(rules, db.Name, "", nil, nil)
		if err != nil {
			return err
		}

		rules = readAuthRules[db.Name+"."+table]
		newRules := []map[string]any{}
		for _, rule := range rules {
			isPending := false
			for key, val := range rule {
				if strings.HasPrefix(key, "$") {
					isPending = true
					break
				}
				if val, ok := val.(string); ok && strings.HasPrefix(val, "$") {
					isPending = true
					break
				}
			}
			if validateData && isPending {
				newRules = append(newRules, rule)
			}
			if !validateData && !isPending {
				newRules = append(newRules, rule)
			}
		}

		if !validateData {
			err = me.validateRules(newRules, db.Name, table, nil, nil)
			if err != nil {
				return err
			}
			continue
		}

		qua := ""
		for _, quer := range query.Keys {
			queryTable := query.Get(quer)
			if queryTable.Name == table {
				qua = quer
			}
		}
		data := raw[qua]
		err = me.validateRules(newRules, db.Name, table, nil, data)
		if err != nil {
			return err
		}
	}

	return nil
}

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
			if cond.Left.Value == nil {
				left := query.Get(cond.Left.Qualifier)
				if left == nil {
					return fmt.Errorf("table not found")
				}
			}
			if cond.Right.Value == nil {
				right := query.Get(cond.Right.Qualifier)
				if right == nil {
					return fmt.Errorf("table not found")
				}
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

type Field struct {
	Qualifier string
	Table     string
	As        string
	Field     string
	Value     any
}

// qualifier -> as -> field
func parseSelectField(stmt *sqlparser.Select, query *models.OrderMap[string, *models.QueryTable]) ([]*Field, error) {
	newFields := []*Field{}
	for _, expr := range stmt.SelectExprs {
		if expr, ok := expr.(*sqlparser.StarExpr); ok {
			qua := expr.TableName.Name.String()
			if qua == "" && len(query.Keys) != 1 {
				return nil, fmt.Errorf("qualifier is must be defined")
			}
			var table *models.QueryTable
			if len(query.Keys) == 1 {
				table = query.Get(query.Keys[0])
			} else {
				table = query.Get(qua)
			}
			if table == nil {
				return nil, fmt.Errorf("table for qualifier %s is not found", qua)
			}
			if qua == "" {
				qua = table.Name
			}
			db := getDb(table.Name)
			fieldInfo := getTableFields(db.Name, table.Name)
			for fieldKey := range fieldInfo {
				newFields = append(newFields, &Field{
					Qualifier: qua,
					Table:     table.Name,
					As:        "",
					Field:     fieldKey,
					Value:     nil,
				})
			}
			continue
		}
		if aliased, ok := expr.(*sqlparser.AliasedExpr); ok {
			if val, ok := aliased.Expr.(*sqlparser.SQLVal); ok {
				val, _ := utils.ParseValue(val)
				as := aliased.As.String()
				if as == "" {
					return nil, fmt.Errorf("select value must be with as")
				}
				field := &Field{
					As:    aliased.As.String(),
					Value: val,
				}
				newFields = append(newFields, field)
				continue
			}
			colName := aliased.Expr.(*sqlparser.ColName)
			qua := colName.Qualifier.Name.String()
			if qua == "" && len(query.Keys) != 1 {
				return nil, fmt.Errorf("qualifier is must be defined")
			}
			var table *models.QueryTable
			if len(query.Keys) == 1 {
				table = query.Get(query.Keys[0])
			} else {
				table = query.Get(qua)
			}
			if table == nil {
				return nil, fmt.Errorf("table for qualifier %s is not found", qua)
			}
			if qua == "" {
				qua = table.Name
			}
			tableField := colName.Name.String()
			field := &Field{
				Qualifier: qua,
				Table:     table.Name,
				As:        aliased.As.String(),
				Field:     tableField,
			}
			newFields = append(newFields, field)
			continue
		}
		fmt.Println("???", reflect.TypeOf(expr))
	}

	return newFields, nil
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

func applyDepWheres(depConds []*models.Cond, qua string, wheres *[]*models.Cond, rawValue map[string][]map[string]any) {
	for _, cond := range depConds {
		if cond.Left.Value != nil || cond.Right.Value != nil {
			*wheres = append(*wheres, cond)
			continue
		}
		if cond.Left.Qualifier == qua {
			result := rawValue[cond.Right.Qualifier]
			check := map[string]bool{}
			values := []any{}
			for _, res := range result {
				val := res[cond.Right.Field]
				key := fmt.Sprint(val)
				_, exist := check[key]
				if exist {
					continue
				}
				check[key] = true
				values = append(values, val)
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
			check := map[string]bool{}
			values := []any{}
			for _, res := range result {
				val := res[cond.Left.Field]
				key := fmt.Sprint(val)
				_, exist := check[key]
				if exist {
					continue
				}
				check[key] = true
				values = append(values, val)
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
			if _id, ok := fieldValue.(primitive.ObjectID); ok {
				fieldValue = _id.Hex()
			}
			key += fmt.Sprint(fieldValue) + "_"
		} else if cond.Right.Qualifier == qua {
			fieldValue := value[cond.Right.Field]
			if _id, ok := fieldValue.(primitive.ObjectID); ok {
				fieldValue = _id.Hex()
			}
			key += fmt.Sprint(fieldValue) + "_"
		}
	}

	return key
}

func generateFinalValue(res []map[string]any, fields []*Field, query *models.OrderMap[string, *models.QueryTable]) []map[string]any {
	final := []map[string]any{}
	for _, re := range res {
		newValue := map[string]any{}
		for _, field := range fields {
			if field.Qualifier == query.Keys[0] {
				as := field.As
				if as == "" {
					as = field.Field
				}
				if field.Value != nil {
					newValue[as] = field.Value
				} else {
					newValue[as] = re[field.Field]
				}
			} else {
				join, ok := re[field.Qualifier].([]any)
				if !ok {
					continue
				}
				joinName := field.Table
				test, exist := newValue[joinName]
				if !exist {
					newJoin := make([]map[string]any, len(join))
					for idx := range join {
						newJoin[idx] = make(map[string]any)
					}
					newValue[joinName] = newJoin
					test = newJoin
				}

				for idx, val := range join {
					val, ok1 := val.(map[string]any)
					a, ok2 := test.([]map[string]any)
					if !ok1 || !ok2 {
						continue
					}
					as := field.As
					if as == "" {
						as = field.Field
					}
					if field.Value != nil {
						a[idx][as] = field.Value
					} else {
						a[idx][as] = val[field.Field]
					}
				}
			}
		}
		final = append(final, newValue)
	}

	return final
}

func processJoin(query *models.OrderMap[string, *models.QueryTable], raw map[string][]map[string]any) []map[string]any {
	for i := 1; i < len(query.Keys); i++ {
		qua := query.Keys[i]
		table := query.Get(qua)
		joinMap := map[string][]any{}
		for _, val := range raw[qua] {
			key := buildKey(table, qua, val)
			if joinMap[key] == nil {
				joinMap[key] = make([]any, 0)
			}
			joinMap[key] = append(joinMap[key], val)
		}

		for _, value := range raw[query.Keys[0]] {
			keys := []string{""}
			for _, cond := range table.DepConds {
				if cond.Left.Qualifier != qua {
					qua := cond.Left.Qualifier
					field := cond.Left.Field
					if qua == query.Keys[0] {
						for idx := range keys {
							keys[idx] += fmt.Sprint(value[field]) + "_"
						}
					} else {
						if val, ok := value[qua].([]any); ok {
							newKeys := []string{}
							for idx := range keys {
								for _, a := range val {
									if a, ok := a.(map[string]any); ok {
										newKeys = append(newKeys, keys[idx]+fmt.Sprint(a[field])+"_")
									}
								}
							}
							keys = newKeys
						} else {
							fmt.Println("???", qua, reflect.TypeOf(value[qua]))
						}
					}
				} else if cond.Right.Qualifier != qua {
					qua := cond.Right.Qualifier
					field := cond.Right.Field
					if qua == query.Keys[0] {
						for idx := range keys {
							keys[idx] += fmt.Sprint(value[field]) + "_"
						}
					} else {
						if val, ok := value[qua].([]any); ok {
							newKeys := []string{}
							for idx := range keys {
								for _, a := range val {
									if a, ok := a.(map[string]any); ok {
										newKeys = append(newKeys, keys[idx]+fmt.Sprint(a[field])+"_")
									}
								}
							}
							keys = newKeys
						} else {
							fmt.Println("???", qua, reflect.TypeOf(value[qua]))
						}
					}
				}
			}
			joinValues := []any{}
			for _, key := range keys {
				joinValues = append(joinValues, joinMap[key]...)
			}
			value[qua] = joinValues
		}
	}

	final := []map[string]any{}
	for _, value := range raw[query.Keys[0]] {
		valid := true
		for i := 1; i < len(query.Keys); i++ {
			qua := query.Keys[i]
			table := query.Get(qua)
			if table.Join != "join" {
				continue
			}
			joinValues, ok := value[qua].([]any)
			if !ok || len(joinValues) == 0 {
				valid = false
			}
		}
		if !valid {
			continue
		}
		final = append(final, value)
	}

	return final
}
