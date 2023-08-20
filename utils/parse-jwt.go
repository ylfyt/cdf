package utils

import (
	"github.com/golang-jwt/jwt/v5"
)

func ParseJwt(tokenString string, secretKey string) map[string]any {
	token, err := jwt.ParseWithClaims(tokenString, &jwt.MapClaims{}, func(token *jwt.Token) (any, error) {
		return []byte(secretKey), nil
	})
	if err != nil || token == nil {
		return nil
	}
	claim, ok := token.Claims.(*jwt.MapClaims)
	if !ok {
		return nil
	}

	temp := map[string]any{}
	for key, val := range *claim {
		temp[key] = val
	}

	return temp
}
