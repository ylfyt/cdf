package utils

func GetMapKeys[T comparable, S any](m map[T]S) []T {
	keys := make([]T, 0)
	for key := range m {
		keys = append(keys, key)
	}

	return keys
}
