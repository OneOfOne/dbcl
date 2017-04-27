package backend

// Backend is *usually* a disk data-store to be used by DBCL.
// Note that once the Backend gets passed to DBCL, you *should* not call Close manually.
type Backend interface {
	LoadAll(LoadFn) error
	Update(UpdateFn) error
	Close() error

	Raw() interface{}
}

type LoadFn func(bucket, key string, value []byte) error
type ApplyFn func(a *Action) error
type UpdateFn func(aFn ApplyFn) error

// Action represents a database action
type Action struct {
	Bucket string
	Key    string
	Value  []byte
	Type   uint8
}

// Action types for a database action
const (
	_ uint8 = iota
	ActionSet
	ActionDelete
	ActionCreateBucket
	ActionDeleteBucket
)
