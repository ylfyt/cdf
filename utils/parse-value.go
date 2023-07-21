package utils

import (
	"errors"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// error -> unsupported value type
func ParseValue(val sqlparser.Expr) (any, error) {
	switch val := val.(type) {
	case *sqlparser.SQLVal:
		strVal := string(val.Val)
		var castedVal any
		switch val.Type {
		case sqlparser.IntVal:
			castedVal, _ = strconv.Atoi(strVal)
		case sqlparser.BitVal:
			castedVal, _ = strconv.ParseBool(strVal)
		case sqlparser.FloatVal:
			castedVal, _ = strconv.ParseFloat(strVal, 64)
		default:
			castedVal = strings.Trim(strVal, `'"`)
		}
		return castedVal, nil
	case sqlparser.BoolVal:
		return val, nil
	case *sqlparser.NullVal:
		return nil, nil
	default:
		return nil, errors.New("unsupported value type")
	}
}
