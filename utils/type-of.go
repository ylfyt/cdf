package utils

import "reflect"

func TypeOf(val any) string {
	return reflect.TypeOf(val).String()
}
