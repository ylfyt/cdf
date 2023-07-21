package handlers

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
)

func InsertMongo(conn *mongo.Database, table string, columns []string, values [][]any) error {
	fmt.Printf("Data: %+v\n", values)

	documents := []any{}
	for _, tuple := range values {
		document := map[string]any{}
		for i := range columns {
			document[columns[i]] = tuple[i]
		}
		documents = append(documents, document)
	}

	_, err := conn.Collection(table).InsertMany(context.TODO(), documents)
	return err
}
