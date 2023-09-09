package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/mongo"
)

func (me *HandlerCtx) MongoInsert(conn *mongo.Database, table string, columns []string, values [][]any) error {
	documents := []any{}
	for _, tuple := range values {
		document := map[string]any{}
		for idx, col := range columns {
			field := me.Fields[col]
			if field.Type != "object" && field.Type != "_object" {
				document[col] = tuple[idx]
				continue
			}
			var tmp any
			err := json.Unmarshal([]byte(fmt.Sprint(tuple[idx])), &tmp)
			if err != nil {
				return err
			}
			if field.Type == "_object" && reflect.TypeOf(tmp).Kind() != reflect.Slice {
				return fmt.Errorf("value of '%s' is not array of object", field)
			}
			if field.Type == "object" && reflect.TypeOf(tmp).Kind() != reflect.Map {
				return fmt.Errorf("value of '%s' is not an object", field)
			}
			document[col] = tmp
		}
		documents = append(documents, document)
	}

	_, err := conn.Collection(table).InsertMany(context.TODO(), documents)
	return err
}
