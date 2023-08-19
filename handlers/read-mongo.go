package handlers

import (
	"cdf/models"
	"context"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func parseOp(op string) string {
	op = strings.ToLower(op)
	switch op {
	case "=":
		return "$eq"
	case ">":
		return "$gt"
	case ">=":
		return "$gte"
	case "<":
		return "$lt"
	case "<=":
		return "$lte"
	case "in":
		return "$in"
	}
	return ""
}

func ReadMongo(conn *mongo.Database, table *models.QueryTable, wheres []*models.Cond) ([]map[string]any, error) {
	coll := conn.Collection(table.Name)

	filter := map[string]any{}
	for _, cond := range wheres {
		if cond.Left.Value != nil && cond.Right.Value != nil {
			//TODO ??
			continue
		}
		op := parseOp(cond.Op)
		field := cond.Left.Field
		val := cond.Right.Value
		filter[field] = bson.M{
			op: val,
		}
	}

	cur, err := coll.Find(context.TODO(), filter)
	if err != nil {
		return nil, err
	}
	result := []map[string]any{}
	for cur.Next(context.TODO()) {
		var val bson.M
		if err := cur.Decode(&val); err != nil {
			return nil, err
		}
		result = append(result, val)
	}

	return result, nil
}
