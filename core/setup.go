package core

import (
	"cdf/handlers"
	"cdf/models"
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type driver struct {
	Type   string
	insert func(conn any, table string, columns []string, values [][]any) error
}

type database struct {
	Info string
	Type string
	Name string
	Conn any
}

var drivers map[string]*driver
var databaseTable map[string]int
var databases []*database

func getConn(dbType string, conn string) (any, error) {
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

func getDb(table string) *database {
	idx, exist := databaseTable[table]
	if !exist {
		return nil
	}

	return databases[idx]
}

func Start(schema *models.Schema) {
	if drivers != nil || databases != nil {
		fmt.Println("DB already initiated")
		return
	}
	drivers = make(map[string]*driver)
	databases = make([]*database, 0)
	databaseTable = make(map[string]int)

	drivers["PostgreSQL"] = &driver{
		Type: "PostgreSQL",
		insert: func(conn any, table string, columns []string, values [][]any) error {
			if pg, ok := conn.(*sql.DB); ok {
				return handlers.InsertPg(pg, table, columns, values)
			}
			return fmt.Errorf("db is not type of PostgreSQL")
		},
	}

	drivers["MongoDB"] = &driver{
		Type: "MongoDB",
		insert: func(conn any, table string, columns []string, values [][]any) error {
			if client, ok := conn.(*mongo.Database); ok {
				return handlers.InsertMongo(client, table, columns, values)
			}

			return fmt.Errorf("db is not type of MongoDB")
		},
	}

	for _, dbInfo := range schema.Databases {
		db, err := getConn(dbInfo.Type, dbInfo.ConnectionString)
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
