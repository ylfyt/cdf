package utils

import (
	"encoding/json"
	"fmt"
)

type placeHolder[T any] struct {
	Val T `json:"val"`
}

func tryParse[T any](jsonString []byte) (T, error) {
	var temp placeHolder[T]
	err := json.Unmarshal(jsonString, &temp)
	return temp.Val, err
}

func CaseInt(val any) (int, error) {
	jsonString := fmt.Sprintf(`{"val": %s}`, fmt.Sprint(val))
	return tryParse[int]([]byte(jsonString))
}

func CaseInt64(val any) (int64, error) {
	jsonString := fmt.Sprintf(`{"val": %s}`, fmt.Sprint(val))
	return tryParse[int64]([]byte(jsonString))
}

func CaseFloat(val any) (float32, error) {
	jsonString := fmt.Sprintf(`{"val": %s}`, fmt.Sprint(val))
	return tryParse[float32]([]byte(jsonString))
}
func CaseFloat64(val any) (float64, error) {
	jsonString := fmt.Sprintf(`{"val": %s}`, fmt.Sprint(val))
	return tryParse[float64]([]byte(jsonString))
}

func CaseString(val any) string {
	return fmt.Sprint(val)
}
