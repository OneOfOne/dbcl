package dbcl

type MarshalFn func(v interface{}) ([]byte, error)
type UnmarshalFn func(data []byte, v interface{}) error
