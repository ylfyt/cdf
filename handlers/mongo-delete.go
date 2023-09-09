package handlers

import (
	"cdf/models"
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func (me *HandlerCtx) MongoDelete(conn *mongo.Database, table string, wheres []*models.Cond) (int, error) {
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
		if field == "_id" {
			if _id, ok := val.(string); ok {
				if objectID, err := primitive.ObjectIDFromHex(_id); err == nil {
					val = objectID
				}
			}
		}
		filter[field] = bson.M{
			op: val,
		}
	}

	res, err := coll.DeleteMany(context.TODO(), filter)
	if err != nil {
		return 0, err
	}

	return int(res.DeletedCount), nil
}
