package api

import (
	"cdf/core"
	"cdf/utils"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/xwb1989/sqlparser"
)

type ResponseDto struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

func sendSuccess(w http.ResponseWriter, data interface{}) error {
	w.Header().Add("content-type", "application/json")
	response := ResponseDto{
		Success: true,
		Message: "",
		Data:    data,
	}
	w.WriteHeader(http.StatusOK)
	err := json.NewEncoder(w).Encode(response)
	return err
}

func sendError(w http.ResponseWriter, message string) error {
	w.Header().Add("content-type", "application/json")
	response := ResponseDto{
		Success: false,
		Message: message,
		Data:    nil,
	}
	w.WriteHeader(http.StatusBadRequest)
	err := json.NewEncoder(w).Encode(response)
	return err
}

func requestHandler(w http.ResponseWriter, r *http.Request) {
	query, err := io.ReadAll(r.Body)
	if err != nil {
		sendError(w, err.Error())
		return
	}

	stmt, err := sqlparser.Parse(string(query))
	if err != nil {
		sendError(w, fmt.Sprintf("Failed to parse query: %v\n", err))
		return
	}

	token := strings.TrimPrefix(r.Header.Get("authorization"), "Bearer ")
	claim := utils.ParseJwt(token, "my-secret-key")

	handler := core.Handler{
		Claim: claim,
	}

	data, err := handler.Execute(stmt)
	if err != nil {
		fmt.Println("Err", err)
		sendError(w, fmt.Sprintf("Failed to parse query: %v\n", err))
		return
	}

	sendSuccess(w, data)
}

func Start() {
	http.HandleFunc("/", requestHandler)

	fmt.Println("Server is listening on port", 8080)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("Failed to listen: %s\n", err)
	}
}
