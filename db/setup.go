package db

import (
	"cdf/models"
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type handler struct {
	Type string
	get  func(conn any, table string, reqMap map[string]any) ([]map[string]any, error)
}

type database struct {
	Info string
	Type string
	Name string
	Conn any
}

var handlers map[string]*handler
var databaseTable map[string]int
var databases []*database

func getDb(dbType string, conn string) (any, error) {
	if dbType == "PostgreSQL" {
		sqlDb, err := sql.Open("postgres", conn)
		return sqlDb, err
	}
	if dbType == "MongoDB" {
		data := strings.Split(conn, "/")
		if len(data) < 4 {
			return nil, fmt.Errorf("please supply db name in the url")
		}

		dbInfo := data[3]
		dbInfos := strings.Split(dbInfo, "?")
		dbName := dbInfos[0]

		opts := options.Client().ApplyURI(conn).SetServerAPIOptions(options.ServerAPI(options.ServerAPIVersion1))

		client, err := mongo.Connect(context.TODO(), opts)
		if err != nil {
			return nil, err
		}

		mongoDb := client.Database(dbName)

		return mongoDb, nil
	}

	return nil, fmt.Errorf("dbtype %s is not found", dbType)
}

func Start(schema *models.Schema) {
	if handlers != nil || databases != nil {
		fmt.Println("DB already initiated")
		return
	}
	handlers = make(map[string]*handler)
	databases = make([]*database, 0)
	databaseTable = make(map[string]int)

	handlers["PostgreSQL"] = &handler{
		Type: "PostgreSQL",
		get: func(conn any, table string, reqMap map[string]any) ([]map[string]any, error) {
			if pg, ok := conn.(*sql.DB); ok {
				fmt.Printf("Data: %+v\n", pg)
				return nil, nil
			}
			return nil, fmt.Errorf("db is not type of PostgreSQL")
		},
	}

	handlers["MongoDB"] = &handler{
		Type: "MongoDB",
		get: func(conn any, table string, reqMap map[string]any) ([]map[string]any, error) {
			if client, ok := conn.(*mongo.Database); ok {
				fmt.Printf("Data: %+v\n", client)
				return nil, nil
			}

			return nil, fmt.Errorf("db is not type of MongoDB")
		},
	}

	for _, dbInfo := range schema.Databases {
		db, err := getDb(dbInfo.Type, dbInfo.ConnectionString)
		if err != nil {
			fmt.Println("Err", err)
			continue
		}
		databases = append(databases, &database{
			Info: dbInfo.ConnectionString,
			Type: dbInfo.Type,
			Name: dbInfo.Name,
			Conn: db,
		})

		for _, table := range dbInfo.Tables {
			databaseTable[table.Name] = len(databases) - 1
		}
	}
}