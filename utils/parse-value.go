package utils

import (
	"errors"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// error -> unsupported value type
func ParseValue(expr sqlparser.Expr) (any, error) {
	switch val := expr.(type) {
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
	case *sqlparser.ConvertExpr:
		if expr, ok := val.Expr.(*sqlparser.SQLVal); ok {
			if val.Type.Type == "signed" {
				castedVal, _ := strconv.Atoi(string(expr.Val))
				return castedVal, nil
			}
			if val.Type.Type == "unsigned" {
				castedVal, _ := strconv.ParseUint(string(expr.Val), 10, 64)
				return castedVal, nil
			}

			return string(expr.Val), nil
		}

		return nil, nil
	default:
		return nil, errors.New("unsupported value type")
	}
}
