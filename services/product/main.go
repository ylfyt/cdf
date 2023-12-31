package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

func get(conn *sql.DB, query string, params ...any) ([]map[string]any, error) {
	rows, err := conn.Query(query, params...)
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

type ResponseDto struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

func main() {
	conStr := "postgresql://postgres:postgres@localhost/db_product?sslmode=disable"
	db, err := sql.Open("postgres", conStr)
	if err != nil {
		panic(err)
	}

	db.SetMaxOpenConns(1000)

	r := gin.Default()
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	r.GET("/product", func(ctx *gin.Context) {
		storeIdStr := ctx.Query("store")
		var stores []int
		if storeIdStr != "" {
			for _, store := range strings.Split(storeIdStr, ",") {
				id, err := strconv.Atoi(store)
				if err != nil {
					fmt.Println("Err", err)
					ctx.JSON(http.StatusInternalServerError, ResponseDto{
						Success: false,
						Message: err.Error(),
					})
					return
				}
				stores = append(stores, id)
			}
		}
		var res []map[string]any
		var err error
		if len(stores) != 0 {
			res, err = get(db, `SELECT * FROM product WHERE store_id = ANY($1)`, pq.Array(stores))
		} else {
			res, err = get(db, `SELECT * FROM product`)
		}
		if err != nil {
			fmt.Println("Err", err)
			ctx.JSON(http.StatusInternalServerError, ResponseDto{
				Success: false,
				Message: err.Error(),
			})
			return
		}

		ctx.JSON(http.StatusOK, ResponseDto{
			Success: true,
			Data:    res,
		})
	})

	r.GET("/product/:id", func(ctx *gin.Context) {
		idStr := ctx.Params.ByName("id")
		id, err := strconv.Atoi(idStr)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, ResponseDto{
				Success: false,
				Message: "id is not valid",
			})
		}
		res, err := get(db, `SELECT * FROM product WHERE id = $1`, id)
		if err != nil {
			fmt.Println("Err", err)
			ctx.JSON(http.StatusInternalServerError, ResponseDto{
				Success: false,
				Message: "",
			})
			return
		}

		ctx.JSON(http.StatusOK, ResponseDto{
			Success: true,
			Data:    res,
		})
	})

	r.Run(":4000")
}
