package utils

func Ternary[T any](cond bool, trueValue T, falseValue T) T {
	if cond {
		return trueValue
	}
	return falseValue
}