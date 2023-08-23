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

var INSERT2 = `INSERT INTO store (user_id, name) VALUES (3, 'Toko 3')`

var DELETE_QUERY = `DELETE FROM stores WHERE _id = '64bef4d717b947ddb0dc725f'`
var UPDATE_QUERY = `UPDATE product SET description = 'Updated Description 1', category='TOY' WHERE name = 'Product 1'`

var DELETE2 = `DELETE FROM store WHERE id = 191`
var DELETE3 = `DELETE FROM product WHERE _id = '64e0671c8a56912f4d847fb6'`

var SELECT1 = `
	SELECT * FROM product p JOIN inventory i ON p._id = i.product_id
`

var SELECT_QUERY = `
	SELECT
		s.id,
		s.name,
		u.name as username,
		o.product_id
	FROM 
		store s
		LEFT JOIN users u ON s.user_id = u.id
		LEFT JOIN orders o ON o.user_id = u.id
	WHERE
		u.id > 1
`

var SELECT_QUERY2 = `
	SELECT 
		s.id,
		s.name,
		p.description 
	FROM 
		store s	
		LEFT JOIN product p ON p.store_id = s.id
		
`

var JWT_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdG9yZV9pZCI6IjEiLCJ1c2VyX2lkIjoiMiIsImFnZSI6IjE1IiwidXNlcm5hbWUiOiJidWRpIn0.ng-e7HkM1t07PpLrPgPv7nlBldgvtlHeUMEQKX7ChTY"

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

	stmt, err := sqlparser.Parse(`
	UPDATE orders 
	SET 
		updated_at = NOW(),
		payment = CONVERT(
			'{"currency": "IDR", "amount": 20000, "type": "BANK-VA", "status": "PAID"}',
			JSON
		)
	WHERE 
		_id = '64e553fdd104247cdf7ce800'
	`)
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
