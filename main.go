package main

import (
	"cdf/core"
	"cdf/models"
	"cdf/utils"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/xwb1989/sqlparser"
)

var INSERT_QUERY = `INSERT INTO orders (name, email) values ('Yudi', 'yudi@gmail.com')`

var INSERT2 = `INSERT INTO store (user_id, name) VALUES (2, 'Toko 3')`

var DELETE_QUERY = `DELETE FROM stores WHERE _id = '64bef4d717b947ddb0dc725f'`
var UPDATE_QUERY = `UPDATE orders SET user_id = 'example@gmail.com' WHERE _id = '64bef4d78c548ee82bc69fd3'`

var SELECT1 = `
	SELECT
		*
	FROM
		store
`

var SELECT_QUERY = `
	SELECT 
		s.*,
		u.name as username,
		u.*
	FROM 
		store s
		JOIN users u ON s.user_id = u.id
		LEFT JOIN orders o ON o.user_id = u.id
`

var SELECT_QUERY2 = `
	SELECT 
		*
	FROM 
		store s	
		JOIN product p ON p.store_id = s.id
		
`

var JWT_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJyb2xlIjoiYWRtaW4iLCJ1c2VyX2lkIjoiMiIsImFnZSI6IjIwIiwidXNlcm5hbWUiOiJidWRpIn0.gZRQn8jFBoS5qiP_ShXO8SBn6TNVlkt9Suwx2_u5fjA"

func main() {
	file, err := os.Open("./schema.json")
	if err != nil {
		panic(err)
	}

	data, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}

	var schema models.Schema
	err = json.Unmarshal(data, &schema)
	if err != nil {
		panic(err)
	}

	core.Start(&schema)

	stmt, err := sqlparser.Parse(INSERT2)
	if err != nil {
		fmt.Println("err", err)
		return
	}

	claim := utils.ParseJwt(JWT_TOKEN, "my-secret-key")
	handler := core.Handler{
		Claim: claim,
	}

	res, err := handler.Execute(stmt)
	if err != nil {
		fmt.Println("err", err)
		return
	}
	fmt.Printf("Data: %+v\n", res)
	// api.Start()
}
