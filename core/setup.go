package core

import (
	"cdf/handlers"
	"cdf/models"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocql/gocql"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type database struct {
	Info string
	Type string
	Name string
	Conn any
}

func (me *database) insert(conn any, table string, columns []string, values [][]any) error {
	fields := getTableFields(me.Name, table)
	ctx := handlers.HandlerCtx{
		Fields: fields,
	}

	return insert(conn, &ctx, me.Type, table, columns, values)
}

func (me *database) delete(conn any, table string, wheres []*models.Cond) (int, error) {
	fields := getTableFields(me.Name, table)
	ctx := handlers.HandlerCtx{
		Fields: fields,
	}
	return delete(conn, &ctx, me.Type, table, wheres)
}
func (me *database) update(conn any, table string, wheres []*models.Cond, values map[string]any) (int, error) {
	fields := getTableFields(me.Name, table)
	ctx := handlers.HandlerCtx{
		Fields: fields,
	}
	return update(conn, &ctx, me.Type, table, wheres, values)
}

func (me *database) read(conn any, table *models.QueryTable, wheres []*models.Cond) ([]map[string]any, error) {
	fields := getTableFields(me.Name, table.Name)
	ctx := handlers.HandlerCtx{
		Fields: fields,
	}
	return read(conn, &ctx, me.Type, table, wheres)
}

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
		if sqlDb != nil {
			sqlDb.SetMaxOpenConns(1000)
		}
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

	if dbType == "MySQL" {
		sqlDb, err := sql.Open("mysql", conn)
		return sqlDb, err
	}

	if dbType == "Cassandra" {
		connInfo := strings.Split(conn, "@")
		username := strings.Split(connInfo[0], ":")[0]
		password := strings.Split(connInfo[0], ":")[1]
		hostInfo := strings.Split(connInfo[1], "/")
		host := strings.Split(hostInfo[0], ":")[0]
		port := strings.Split(hostInfo[0], ":")[1]
		keySpace := hostInfo[1]

		cluster := gocql.NewCluster(host)
		cluster.Port, _ = strconv.Atoi(port)
		cluster.Keyspace = keySpace
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
		session, err := cluster.CreateSession()
		return session, err
	}

	return nil, fmt.Errorf("dbtype %s is not supported", dbType)
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

func insert(conn any, ctx *handlers.HandlerCtx, dbType string, table string, columns []string, values [][]any) error {
	if dbType == "PostgreSQL" {
		if conn, ok := conn.(*sql.DB); ok {
			return ctx.PgInsert(conn, table, columns, values)
		}
		return fmt.Errorf("db is not type of PostgreSQL")
	}
	if dbType == "MySQL" {
		if conn, ok := conn.(*sql.DB); ok {
			return ctx.MyInsert(conn, table, columns, values)
		}
		return fmt.Errorf("db is not type of MySQL")
	}
	if dbType == "Cassandra" {
		if conn, ok := conn.(*gocql.Session); ok {
			return ctx.CsInsert(conn, table, columns, values)
		}
		return fmt.Errorf("db is not type of Cassandra")
	}
	if dbType == "MongoDB" {
		if conn, ok := conn.(*mongo.Database); ok {
			return ctx.MongoInsert(conn, table, columns, values)
		}
		return fmt.Errorf("db is not type of MongoDB")
	}
	return fmt.Errorf("db with type of '%s' is not found", dbType)
}

func delete(conn any, ctx *handlers.HandlerCtx, dbType string, table string, wheres []*models.Cond) (int, error) {
	if dbType == "PostgreSQL" {
		if conn, ok := conn.(*sql.DB); ok {
			return ctx.PgDelete(conn, table, wheres)
		}
		return 0, fmt.Errorf("db is not type of PostgreSQL")
	}
	if dbType == "MySQL" {
		if conn, ok := conn.(*sql.DB); ok {
			return ctx.MyDelete(conn, table, wheres)
		}
		return 0, fmt.Errorf("db is not type of MySQL")
	}
	if dbType == "Cassandra" {
		if conn, ok := conn.(*gocql.Session); ok {
			return ctx.CsDelete(conn, table, wheres)
		}
		return 0, fmt.Errorf("db is not type of Cassandra")
	}
	if dbType == "MongoDB" {
		if conn, ok := conn.(*mongo.Database); ok {
			return ctx.MongoDelete(conn, table, wheres)
		}
		return 0, fmt.Errorf("db is not type of MongoDB")
	}
	return 0, fmt.Errorf("db with type of '%s' is not found", dbType)
}

func update(conn any, ctx *handlers.HandlerCtx, dbType string, table string, wheres []*models.Cond, values map[string]any) (int, error) {
	if dbType == "PostgreSQL" {
		if conn, ok := conn.(*sql.DB); ok {
			return ctx.PgUpdate(conn, table, wheres, values)
		}
		return 0, fmt.Errorf("db is not type of PostgreSQL")
	}
	if dbType == "MySQL" {
		if conn, ok := conn.(*sql.DB); ok {
			return ctx.MyUpdate(conn, table, wheres, values)
		}
		return 0, fmt.Errorf("db is not type of MySQL")
	}
	if dbType == "Cassandra" {
		if conn, ok := conn.(*gocql.Session); ok {
			return ctx.CsUpdate(conn, table, wheres, values)
		}
		return 0, fmt.Errorf("db is not type of Cassandra")
	}
	if dbType == "MongoDB" {
		if conn, ok := conn.(*mongo.Database); ok {
			return ctx.MongoUpdate(conn, table, wheres, values)
		}
		return 0, fmt.Errorf("db is not type of MongoDB")
	}
	return 0, fmt.Errorf("db with type of '%s' is not found", dbType)
}

func read(conn any, ctx *handlers.HandlerCtx, dbType string, table *models.QueryTable, wheres []*models.Cond) ([]map[string]any, error) {
	if dbType == "PostgreSQL" {
		if conn, ok := conn.(*sql.DB); ok {
			return ctx.PgRead(conn, table, wheres)
		}
		return nil, fmt.Errorf("db is not type of PostgreSQL")
	}
	if dbType == "MySQL" {
		if conn, ok := conn.(*sql.DB); ok {
			return ctx.MyRead(conn, table, wheres)
		}
		return nil, fmt.Errorf("db is not type of MySQL")
	}
	if dbType == "Cassandra" {
		if conn, ok := conn.(*gocql.Session); ok {
			return ctx.CsRead(conn, table, wheres)
		}
		return nil, fmt.Errorf("db is not type of Cassandra")
	}
	if dbType == "MongoDB" {
		if conn, ok := conn.(*mongo.Database); ok {
			return ctx.MongoRead(conn, table, wheres)
		}
		return nil, fmt.Errorf("db is not type of MongoDB")
	}
	return nil, fmt.Errorf("db with type of '%s' is not found", dbType)
}

func Start(dbschema *models.Schema) {
	if databases != nil {
		fmt.Println("DB already initiated")
		return
	}
	schema = dbschema
	databases = make([]*database, 0)
	databaseTable = make(map[string]int)

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
