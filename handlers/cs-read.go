package handlers

import (
	"cdf/models"
	"cdf/utils"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func buildWhereQuery(wheres []*models.Cond, queryParams *[]any, isDollarPlaceholder bool) []string {
	whereQueries := []string{}
	for _, cond := range wheres {
		query := ""
		if cond.Left.Value != nil && cond.Right.Value != nil {
			// TODO:
		} else if cond.Left.Value != nil {
			// TODO: Check if deps or not
			if vals, ok := cond.Left.Value.([]any); ok {
				if len(vals) == 0 {
					query = "FALSE"
				} else {
					right := ""
					for idx, val := range vals {
						if _id, ok := val.(primitive.ObjectID); ok {
							*queryParams = append(*queryParams, _id.Hex())
						} else {
							*queryParams = append(*queryParams, val)
						}
						right += utils.Ternary(!isDollarPlaceholder, "?", fmt.Sprintf("$%d", len(*queryParams)))
						if idx != len(vals)-1 {
							right += ","
						}
					}
					query = fmt.Sprintf("%s IN (%s)", cond.Right.Field, right)
				}
			} else {
				if _id, ok := cond.Left.Value.(primitive.ObjectID); ok {
					*queryParams = append(*queryParams, _id.Hex())
				} else {
					*queryParams = append(*queryParams, cond.Left.Value)
				}
				query = fmt.Sprintf("%s %s %s",
					cond.Right.Field,
					cond.Op,
					utils.Ternary(!isDollarPlaceholder, "?", fmt.Sprintf("$%d", len(*queryParams))),
				)
			}
		} else if cond.Right.Value != nil {
			if vals, ok := cond.Right.Value.([]any); ok {
				if len(vals) == 0 {
					query = "FALSE"
				} else {
					right := ""
					for idx, val := range vals {
						if _id, ok := val.(primitive.ObjectID); ok {
							*queryParams = append(*queryParams, _id.Hex())
						} else {
							*queryParams = append(*queryParams, val)
						}
						right += utils.Ternary(!isDollarPlaceholder, "?", fmt.Sprintf("$%d", len(*queryParams)))
						if idx != len(vals)-1 {
							right += ","
						}
					}
					query = fmt.Sprintf("%s IN (%s)", cond.Left.Field, right)
				}
			} else {
				if _id, ok := cond.Right.Value.(primitive.ObjectID); ok {
					*queryParams = append(*queryParams, _id.Hex())
				} else {
					*queryParams = append(*queryParams, cond.Right.Value)
				}
				query = fmt.Sprintf("%s %s %s",
					cond.Left.Field,
					cond.Op,
					utils.Ternary(!isDollarPlaceholder, "?", fmt.Sprintf("$%d", len(*queryParams))),
				)
			}
		} else {
			query = fmt.Sprintf("%s %s %s", cond.Left.Field, cond.Op, cond.Right.Field)
		}
		whereQueries = append(whereQueries, query)
	}
	return whereQueries
}

func CsRead(conn *gocql.Session, table *models.QueryTable, wheres []*models.Cond) ([]map[string]any, error) {
	selects := []string{}
	if len(table.SelectFields) == 0 {
		selects = append(selects, "*")
	}
	for as, field := range table.SelectFields {
		selects = append(selects, fmt.Sprintf("%s AS %s", field, as))
	}

	queryParams := []any{}
	whereQueries := buildWhereQuery(wheres, &queryParams, false)

	query := fmt.Sprintf(`
		SELECT
			%s
		FROM
			%s
		%s
		ALLOW FILTERING
	`,
		strings.Join(selects, ","),
		table.Name,
		utils.Ternary(
			len(whereQueries) == 0,
			"",
			fmt.Sprintf("WHERE %s", strings.Join(whereQueries, " AND ")),
		),
	)

	fmt.Println("=== READ CS ===")
	fmt.Println(query)
	fmt.Println("*** READ CS ***")

	iter := conn.Query(query, queryParams...).Iter()

	var result []map[string]any
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}
		result = append(result, row)
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return result, nil
}
