package main

import (
	"database/sql"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

func get(conn *sql.DB, query string) ([]map[string]any, error) {
	rows, err := conn.Query(query)
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	numOfColumns := len(columns)
	scans := make([]any, numOfColumns)
	scansPtr := make([]any, numOfColumns)

	for i := range scans {
		scansPtr[i] = &scans[i]
	}

	var result []map[string]any
	for rows.Next() {
		err := rows.Scan(scansPtr...)
		if err != nil {
			return nil, err
		}
		row := make(map[string]any)
		for i, v := range columns {

			row[v] = scans[i]
		}
		result = append(result, row)
	}
	return result, nil
}

func main() {
	conStr := "postgresql://postgres:postgres@localhost/db_product?sslmode=disable"
	db, err := sql.Open("postgres", conStr)
	if err != nil {
		panic(err)
	}

	res, err := get(db, `SELECT * FROM product`)
	if err != nil {
		fmt.Println("Err", err)
	}
	fmt.Printf("Data: %+v\n", res)

	r := gin.Default()
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	r.GET("/product", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{
			"hehe": "dsadsa",
		})
	})

	r.Run()
}
