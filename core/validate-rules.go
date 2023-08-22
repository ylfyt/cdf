package core

import (
	"cdf/utils"
	"fmt"
	"strings"
)

func isValid(val1 any, val2 any, fieldType string, op string) error {
	if fieldType == "int" {
		a, _ := utils.CaseInt64(val1)
		if op == "$in" || op == "$nin" {
			if _, ok := val2.([]any); !ok {
				return fmt.Errorf("val2 %s is not array", utils.CaseString(val2))
			}
			vals := val2.([]any)
			found := false
			for _, val := range vals {
				b, _ := utils.CaseInt64(val)
				if op == "$in" && a == b {
					found = true
					break
				}
				if op == "$nin" && a == b {
					return fmt.Errorf("%s in %s", utils.CaseString(val1), utils.CaseString(val2))
				}
			}
			if op == "$in" && !found {
				return fmt.Errorf("%s not in %s", utils.CaseString(val1), utils.CaseString(val2))
			}
			return nil
		}
		b, _ := utils.CaseInt64(val2)
		switch op {
		case "$eq":
			if a != b {
				return fmt.Errorf("%s not equal %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$neq":
			if a == b {
				return fmt.Errorf("%s equal %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$gt":
			if !(a > b) {
				return fmt.Errorf("%s not gte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$gte":
			if !(a >= b) {
				return fmt.Errorf("%s not gte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$lt":
			if !(a < b) {
				return fmt.Errorf("%s not lt %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$lte":
			if !(a <= b) {
				return fmt.Errorf("%s not lte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$in":
		default:
			return fmt.Errorf("operator %s not supported", op)
		}
	} else if fieldType == "float" {
		a, _ := utils.CaseFloat64(val1)
		if op == "$in" || op == "$nin" {
			if _, ok := val2.([]any); !ok {
				return fmt.Errorf("val2 %s is not array", utils.CaseString(val2))
			}
			vals := val2.([]any)
			found := false
			for _, val := range vals {
				b, _ := utils.CaseFloat64(val)
				if op == "$in" && a == b {
					found = true
					break
				}
				if op == "$nin" && a == b {
					return fmt.Errorf("in %s | %s", utils.CaseString(val1), utils.CaseString(val2))
				}
			}
			if op == "$in" && !found {
				return fmt.Errorf("not in %s | %s", utils.CaseString(val1), utils.CaseString(val2))
			}
			return nil
		}
		b, _ := utils.CaseFloat64(val2)
		switch op {
		case "$eq":
			if a != b {
				return fmt.Errorf("%s not equal %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$neq":
			if a == b {
				return fmt.Errorf("%s equal %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$gt":
			if !(a > b) {
				return fmt.Errorf("%s not gte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$gte":
			if !(a >= b) {
				return fmt.Errorf("%s not gte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$lt":
			if !(a < b) {
				return fmt.Errorf("%s not lt %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$lte":
			if !(a <= b) {
				return fmt.Errorf("%s not lte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$in":
		default:
			return fmt.Errorf("operator %s not supported", op)
		}
	} else if fieldType == "string" {
		a := utils.CaseString(val1)
		if op == "$in" || op == "$nin" {
			if _, ok := val2.([]any); !ok {
				return fmt.Errorf("val2 %s is not array", utils.CaseString(val2))
			}
			vals := val2.([]any)
			found := false
			for _, val := range vals {
				b := utils.CaseString(val)
				if op == "$in" && a == b {
					found = true
					break
				}
				if op == "$nin" && a == b {
					return fmt.Errorf("in %s | %s", utils.CaseString(val1), utils.CaseString(val2))
				}
			}
			if op == "$in" && !found {
				return fmt.Errorf("not in %s | %s", utils.CaseString(val1), utils.CaseString(val2))
			}
			return nil
		}
		b := utils.CaseString(val2)
		switch op {
		case "$eq":
			if a != b {
				return fmt.Errorf("%s not equal %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$neq":
			if a == b {
				return fmt.Errorf("%s equal %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$gt":
			if !(a > b) {
				return fmt.Errorf("%s not gte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$gte":
			if !(a >= b) {
				return fmt.Errorf("%s not gte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$lt":
			if !(a < b) {
				return fmt.Errorf("%s not lt %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$lte":
			if !(a <= b) {
				return fmt.Errorf("%s not lte %s", utils.CaseString(val1), utils.CaseString(val2))
			}
		case "$in":
		default:
			return fmt.Errorf("operator %s not supported", op)
		}
	} else {
		return fmt.Errorf("field type %s not supported", fieldType)
	}

	return nil
}

func (me *Handler) validateRules(rules []map[string]any, dbName string, tableName string, inputValues []map[string]any, existValues []map[string]any) error {
	for _, rule := range rules {
		for key, authRule := range rule {
			if rule, ok := authRule.(string); ok && strings.HasPrefix(rule, "data.") {
				tmp := key
				key = rule
				authRule = tmp
			}
			if strings.HasPrefix(key, "data.") {
				if tableName == "" {
					return fmt.Errorf("in db ctx")
				}
				field := strings.Split(key, ".")[1]
				fieldType := getFieldType(dbName, tableName, field)
				if fieldType == "" {
					return fmt.Errorf("field %s not found", field)
				}

				for _, values := range inputValues {
					dataValue, exist := values[field]
					_ = exist
					// if !exist {
					// 	return fmt.Errorf("input field %s not found", field)
					// }

					if val, ok := authRule.(map[string]any); ok {
						for op, val := range val {
							err := isValid(dataValue, val, fieldType, op)
							if err != nil {
								return err
							}
						}
						continue
					}
					if val, ok := authRule.(string); ok && strings.HasPrefix(val, "auth.") {
						claimField := strings.Split(val, ".")[1]
						if me.Claim == nil {
							return fmt.Errorf("unauth")
						}
						authVal, exist := me.Claim[claimField]
						if !exist {
							return fmt.Errorf("claim field %s not found", claimField)
						}

						err := isValid(dataValue, authVal, fieldType, "$eq")
						if err != nil {
							return err
						}
						continue
					}

					err := isValid(dataValue, authRule, fieldType, "$eq")
					if err != nil {
						return err
					}
				}

				continue
			}

			if strings.HasPrefix(key, "auth.") {
				claimField := strings.Split(key, ".")[1]
				if me.Claim == nil {
					return fmt.Errorf("unauth")
				}
				claimVal, exist := me.Claim[claimField]
				if !exist {
					return fmt.Errorf("claim field %s not found", claimField)
				}
				claimType := getValueType(claimVal)
				if mapVal, ok := authRule.(map[string]any); ok {
					for op, val := range mapVal {
						err := isValid(claimVal, val, claimType, op)
						if err != nil {
							return err
						}
					}
					continue
				}
				err := isValid(claimVal, authRule, claimType, "$eq")
				if err != nil {
					return err
				}
				continue
			}

			if strings.HasPrefix(key, "$") {
				if tableName == "" {
					return fmt.Errorf("in db ctx")
				}
				field := strings.Split(key, "$")[1]
				fieldType := getFieldType(dbName, tableName, field)
				if fieldType == "" {
					return fmt.Errorf("field %s not found", field)
				}
				for _, values := range existValues {
					fieldVal, exist := values[field]
					if !exist {
						return fmt.Errorf("field %s is not found", field)
					}
					if val, ok := authRule.(map[string]any); ok {
						for op, val := range val {
							err := isValid(fieldVal, val, fieldType, op)
							if err != nil {
								return err
							}
						}
						continue
					}
					if val, ok := authRule.(string); ok && strings.HasPrefix(val, "auth.") {
						claimField := strings.Split(val, ".")[1]
						if me.Claim == nil {
							return fmt.Errorf("unauth")
						}
						authVal, exist := me.Claim[claimField]
						if !exist {
							return fmt.Errorf("claim field %s not found", claimField)
						}

						err := isValid(fieldVal, authVal, fieldType, "$eq")
						if err != nil {
							return err
						}
						continue
					}
					err := isValid(fieldVal, authRule, fieldType, "$eq")
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}
