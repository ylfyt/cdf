package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
)

func get(conn *gocql.Session, query string, params ...any) ([]map[string]any, error) {
	iter := conn.Query(query, params...).Iter()

	var result []map[string]any
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
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
	conStr := ":@127.0.0.1:9042/db_users"
	connInfo := strings.Split(conStr, "@")
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
	db, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	client := &http.Client{}

	r := gin.Default()
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	r.GET("/user", func(ctx *gin.Context) {
		res, err := get(db, `SELECT * FROM users`)
		if err != nil {
			fmt.Println("Err", err)
			ctx.JSON(http.StatusInternalServerError, ResponseDto{
				Success: false,
				Message: err.Error(),
			})
			return
		}

		userMap := map[string]bool{}
		for _, user := range res {
			if user["id"] == nil {
				continue
			}
			userMap[fmt.Sprint(user["id"])] = true
		}

		userIds := []string{}
		for user := range userMap {
			userIds = append(userIds, user)
		}

		url := fmt.Sprintf("http://localhost:5000/store?user=%s", strings.Join(userIds, ","))
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

		stores, ok := result.Data.([]any)
		if !ok {
			fmt.Println("===================", reflect.TypeOf(result.Data))
			ctx.JSON(http.StatusInternalServerError, ResponseDto{
				Success: false,
				Message: "?????",
			})
			return
		}

		for _, user := range res {
			userStores := []map[string]any{}
			for _, store := range stores {
				store, ok := store.(map[string]any)
				if !ok {
					continue
				}
				if fmt.Sprint(user["id"]) == fmt.Sprint(store["user_id"]) {
					userStores = append(userStores, store)
				}
			}
			user["store"] = userStores
		}

		ctx.JSON(http.StatusOK, ResponseDto{
			Success: true,
			Data:    res,
		})
	})

	r.GET("/user/:id", func(ctx *gin.Context) {
		idStr := ctx.Params.ByName("id")
		id, err := strconv.Atoi(idStr)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, ResponseDto{
				Success: false,
				Message: "id is not valid",
			})
		}
		res, err := get(db, `SELECT * FROM users WHERE id = $1`, id)
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

	r.Run(":6000")
}
