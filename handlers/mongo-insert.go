package handlers

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
)

func (me *HandlerCtx) MongoInsert(conn *mongo.Database, table string, columns []string, values [][]any) error {
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
