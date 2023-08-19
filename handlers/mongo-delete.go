package handlers

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func MongoDelete(conn *mongo.Database, table string, wheres map[string]any) (int, error) {
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

	res, err := coll.DeleteMany(context.TODO(), wheres)
	if err != nil {
		return 0, err
	}

	return int(res.DeletedCount), nil
}
