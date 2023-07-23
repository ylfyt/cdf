package handlers

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func UpdateMongo(conn *mongo.Database, table string, wheres map[string]any, values map[string]any) (int, error) {
	coll := conn.Collection(table)
	if coll == nil {
		return 0, fmt.Errorf("collection %s not found", table)
	}

	if _id, exist := wheres["_id"]; exist {
		if _id, ok := _id.(string); ok {
			if objectID, err := primitive.ObjectIDFromHex(_id); err == nil {
				wheres["_id"] = objectID
			}
		}
	}

	update := bson.M{"$set": values}
	res, err := coll.UpdateMany(context.TODO(), wheres, update)
	if err != nil {
		return 0, err
	}

	return int(res.ModifiedCount), nil
}
