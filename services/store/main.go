package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
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
			if val, ok := scans[i].([]byte); ok {
				row[v] = string(val)
			} else {
				row[v] = scans[i]
			}
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
	conStr := "root:root@tcp(127.0.0.1:3306)/db_store"
	db, err := sql.Open("mysql", conStr)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(1000)

	client := &http.Client{}

	r := gin.Default()
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	r.GET("/store", func(ctx *gin.Context) {
		storeIdStr := ctx.Query("user")
		userId := 0
		if storeIdStr != "" {
			userIdTmp, err := strconv.Atoi(storeIdStr)
			if err != nil {
				fmt.Println("Err", err)
				ctx.JSON(http.StatusInternalServerError, ResponseDto{
					Success: false,
					Message: err.Error(),
				})
				return
			}
			userId = userIdTmp
		}

		var res []map[string]any
		var err error
		if userId != 0 {
			res, err = get(db, `SELECT * FROM store WHERE user_id = ?`, userId)
		} else {
			res, err = get(db, `SELECT * FROM store`)
		}
		if err != nil {
			fmt.Println("Err", err)
			ctx.JSON(http.StatusInternalServerError, ResponseDto{
				Success: false,
				Message: err.Error(),
			})
			return
		}
		for _, store := range res {
			url := fmt.Sprintf("http://localhost:4000/product?store=%s", fmt.Sprint(store["id"]))
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				fmt.Println("Err", err)
				ctx.JSON(http.StatusInternalServerError, ResponseDto{
					Success: false,
					Message: err.Error(),
				})
				return
			}

			// Send the request
			resp, err := client.Do(req)
			if err != nil {
				fmt.Println("Err", err)
				ctx.JSON(http.StatusInternalServerError, ResponseDto{
					Success: false,
					Message: err.Error(),
				})
				return
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				fmt.Println("Err", err)
				ctx.JSON(http.StatusInternalServerError, ResponseDto{
					Success: false,
					Message: err.Error(),
				})
				return
			}

			var result ResponseDto
			err = json.Unmarshal(body, &result)
			if err != nil {
				fmt.Println("Err", err)
				ctx.JSON(http.StatusInternalServerError, ResponseDto{
					Success: false,
					Message: err.Error(),
				})
				return
			}

			if !result.Success {
				fmt.Println("???????", result.Message)
				ctx.JSON(http.StatusInternalServerError, ResponseDto{
					Success: false,
					Message: result.Message,
				})
				return
			}

			store["product"] = result.Data
		}

		ctx.JSON(http.StatusOK, ResponseDto{
			Success: true,
			Data:    res,
		})
	})

	r.GET("/store/:id", func(ctx *gin.Context) {
		idStr := ctx.Params.ByName("id")
		id, err := strconv.Atoi(idStr)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, ResponseDto{
				Success: false,
				Message: "id is not valid",
			})
		}
		res, err := get(db, `SELECT * FROM store WHERE id = ?`, id)
		if err != nil {
			fmt.Println("Err", err)
			ctx.JSON(http.StatusInternalServerError, ResponseDto{
				Success: false,
				Message: "",
			})
			return
		}

		for _, store := range res {
			url := fmt.Sprintf("http://localhost:4000/product?store=%s", fmt.Sprint(store["id"]))
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				fmt.Println("Err", err)
				ctx.JSON(http.StatusInternalServerError, ResponseDto{
					Success: false,
					Message: err.Error(),
				})
				return
			}

			// Send the request
			resp, err := client.Do(req)
			if err != nil {
				fmt.Println("Err", err)
				ctx.JSON(http.StatusInternalServerError, ResponseDto{
					Success: false,
					Message: err.Error(),
				})
				return
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				fmt.Println("Err", err)
				ctx.JSON(http.StatusInternalServerError, ResponseDto{
					Success: false,
					Message: err.Error(),
				})
				return
			}

			var result ResponseDto
			err = json.Unmarshal(body, &result)
			if err != nil {
				fmt.Println("Err", err)
				ctx.JSON(http.StatusInternalServerError, ResponseDto{
					Success: false,
					Message: err.Error(),
				})
				return
			}

			if !result.Success {
				fmt.Println("???????", result.Message)
				ctx.JSON(http.StatusInternalServerError, ResponseDto{
					Success: false,
					Message: result.Message,
				})
				return
			}

			store["product"] = result.Data
		}

		ctx.JSON(http.StatusOK, ResponseDto{
			Success: true,
			Data:    res,
		})
	})

	r.Run(":5000")
}
