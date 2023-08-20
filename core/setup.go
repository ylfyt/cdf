package core

import (
	"cdf/handlers"
	"cdf/models"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type driver struct {
	Type   string
	insert func(conn any, table string, columns []string, values [][]any) error
	delete func(conn any, table string, wheres map[string]any) (int, error)
	update func(conn any, table string, wheres map[string]any, values map[string]any) (int, error)
	read   func(conn any, table *models.QueryTable, wheres []*models.Cond) ([]map[string]any, error)
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
var schema *models.Schema

var createAuthRules map[string][]map[string]any
var updateAuthRules map[string][]map[string]any
var deleteAuthRules map[string][]map[string]any
var readAuthRules map[string][]map[string]any

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

func applyAuth(auths []models.Auth, ctx string) {
	for _, auth := range auths {
		if strings.Contains(auth.Action, "c") {
			createAuthRules[ctx] = append(createAuthRules[ctx], auth.Rule)
		}
		if strings.Contains(auth.Action, "u") {
			updateAuthRules[ctx] = append(updateAuthRules[ctx], auth.Rule)
		}
		if strings.Contains(auth.Action, "d") {
			deleteAuthRules[ctx] = append(updateAuthRules[ctx], auth.Rule)
		}
		if strings.Contains(auth.Action, "r") {
			readAuthRules[ctx] = append(updateAuthRules[ctx], auth.Rule)
		}
	}
}

func Start(dbschema *models.Schema) {
	if drivers != nil || databases != nil {
		fmt.Println("DB already initiated")
		return
	}
	schema = dbschema
	drivers = make(map[string]*driver)
	databases = make([]*database, 0)
	databaseTable = make(map[string]int)

	drivers["PostgreSQL"] = &driver{
		Type: "PostgreSQL",
		insert: func(conn any, table string, columns []string, values [][]any) error {
			if conn, ok := conn.(*sql.DB); ok {
				return handlers.PgInsert(conn, table, columns, values)
			}
			return fmt.Errorf("db is not type of PostgreSQL")
		},
		delete: func(conn any, table string, wheres map[string]any) (int, error) {
			if conn, ok := conn.(*sql.DB); ok {
				return handlers.PgDelete(conn, table, wheres)
			}
			return 0, fmt.Errorf("db is not type of PostgreSQL")
		},
		update: func(conn any, table string, wheres map[string]any, values map[string]any) (int, error) {
			if conn, ok := conn.(*sql.DB); ok {
				return handlers.PgUpdate(conn, table, wheres, values)
			}
			return 0, fmt.Errorf("db is not type of PostgreSQL")
		},
		read: func(conn any, table *models.QueryTable, wheres []*models.Cond) ([]map[string]any, error) {
			if conn, ok := conn.(*sql.DB); ok {
				return handlers.PgRead(conn, table, wheres)
			}
			return nil, fmt.Errorf("db is not type of PostgreSQL")
		},
	}

	drivers["MongoDB"] = &driver{
		Type: "MongoDB",
		insert: func(conn any, table string, columns []string, values [][]any) error {
			if conn, ok := conn.(*mongo.Database); ok {
				return handlers.MongoInsert(conn, table, columns, values)
			}

			return fmt.Errorf("db is not type of MongoDB")
		},
		delete: func(conn any, table string, wheres map[string]any) (int, error) {
			if conn, ok := conn.(*mongo.Database); ok {
				return handlers.MongoDelete(conn, table, wheres)
			}

			return 0, fmt.Errorf("db is not type of MongoDB")
		},
		update: func(conn any, table string, wheres map[string]any, values map[string]any) (int, error) {
			if conn, ok := conn.(*mongo.Database); ok {
				return handlers.MongoUpdate(conn, table, wheres, values)
			}

			return 0, fmt.Errorf("db is not type of MongoDB")
		},
		read: func(conn any, table *models.QueryTable, wheres []*models.Cond) ([]map[string]any, error) {
			if conn, ok := conn.(*mongo.Database); ok {
				return handlers.MongoRead(conn, table, wheres)
			}
			return nil, fmt.Errorf("db is not type of MongoDB")
		},
	}

	createAuthRules = make(map[string][]map[string]any)
	updateAuthRules = make(map[string][]map[string]any)
	deleteAuthRules = make(map[string][]map[string]any)
	readAuthRules = make(map[string][]map[string]any)

	for _, dbInfo := range dbschema.Databases {
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
		applyAuth(dbInfo.Auths, dbInfo.Name)

		for _, table := range dbInfo.Tables {
			databaseTable[table.Name] = len(databases) - 1
			applyAuth(table.Auths, dbInfo.Name+"."+table.Name)

			for name, field := range table.Fields {
				if field, ok := field.(map[string]any); ok {
					auth := field["auth"]
					if auth == nil {
						continue
					}
					data, _ := json.Marshal(auth)
					var auths []models.Auth
					_ = json.Unmarshal(data, &auths)
					applyAuth(auths, dbInfo.Name+"."+table.Name+"."+name)
				}
			}
		}
	}
	// fmt.Printf("Create %+v\n", createAuthRules)
	// fmt.Printf("Update %+v\n", updateAuthRules)
	// fmt.Printf("Delete %+v\n", deleteAuthRules)
	// fmt.Printf("Read %+v\n", readAuthRules)
}
