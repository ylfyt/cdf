package handlers

import (
	"cdf/models"
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func (me *HandlerCtx) MongoUpdate(conn *mongo.Database, table string, wheres []*models.Cond, values map[string]any) (int, error) {
	coll := conn.Collection(table)
	if coll == nil {
		return 0, fmt.Errorf("collection %s not found", table)
	}

	filter := map[string]any{}
	for _, cond := range wheres {
		if cond.Left.Value != nil && cond.Right.Value != nil {
			//TODO ??
			continue
		}
		op := parseOp(cond.Op)
		field := cond.Left.Field
		val := cond.Right.Value
		fieldInfo := me.Fields[field]
		if fieldInfo.Type == "ObjectID" {
			if objectID, err := primitive.ObjectIDFromHex(fmt.Sprint(val)); err == nil {
				val = objectID
			}
		}

		filter[field] = bson.M{
			op: val,
		}
	}

	for field := range values {
		fieldInfo := me.Fields[field]
		if fieldInfo.Type != "object" && fieldInfo.Type != "_object" {
			continue
		}
		var tmp any
		err := json.Unmarshal([]byte(fmt.Sprint(values[field])), &tmp)
		if err != nil {
			return 0, err
		}

		if fieldInfo.Type == "_object" && reflect.TypeOf(tmp).Kind() != reflect.Slice {
			return 0, fmt.Errorf("value of '%s' is not array of object", field)
		}
		if fieldInfo.Type == "object" && reflect.TypeOf(tmp).Kind() != reflect.Map {
			return 0, fmt.Errorf("value of '%s' is not an object", field)
		}

		values[field] = tmp
	}

	update := bson.M{"$set": values}
	res, err := coll.UpdateMany(context.TODO(), filter, update)
	if err != nil {
		return 0, err
	}

	return int(res.ModifiedCount), nil
}
