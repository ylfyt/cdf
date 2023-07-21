package handlers

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
)

func DeleteMongo(conn *mongo.Database, table string, wheres map[string]string) (int, error) {
	coll := conn.Collection(table)
	if coll == nil {
		return 0, fmt.Errorf("collection %s not found", table)
	}

	fmt.Printf("Data: %+v\n", wheres)

	res, err := coll.DeleteMany(context.TODO(), wheres)
	if err != nil {
		return 0, err
	}

	return int(res.DeletedCount), nil
}
