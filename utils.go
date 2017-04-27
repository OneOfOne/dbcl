package dbcl

import "fmt"

type MarshalFn func(v interface{}) ([]byte, error)
type UnmarshalFn func(data []byte, v interface{}) error

func MarshalRaw(v interface{}) ([]byte, error) {
	switch v := v.(type) {
	case []byte:
		return v, nil
	case *[]byte:
		return *v, nil
	case string:
		return []byte(v), nil
	case *string:
		return []byte(*v), nil
	default:
		return nil, fmt.Errorf("invalid type: %T", v)
	}
}

func UnmarshalRaw(data []byte, v interface{}) error {
	switch v := v.(type) {
	case *[]byte:
		*v = data
	case *string:
		*v = string(data)
	default:
		return fmt.Errorf("invalid type: %T", v)
	}

	return nil
}
