package dbcl

import (
	"sync"

	"github.com/OneOfOne/dbcl/backend"
)

func newBucket(name string, mFn MarshalFn, uFn UnmarshalFn) *Bucket {
	return &Bucket{
		name: name,
		data: make(map[string][]byte),
		tmp:  make(map[string][]byte),

		mFn: mFn,
		uFn: uFn,
	}
}

type Bucket struct {
	name string
	data map[string][]byte
	tmp  map[string][]byte

	mFn MarshalFn
	uFn UnmarshalFn

	m  sync.RWMutex
	tx *Tx
}

func (b *Bucket) Get(key string, out interface{}) error {
	if b.tx != nil {
		if v, ok := b.tmp[key]; ok {
			if v == nil {
				return ErrNotFound
			}
			return b.uFn(v, out)
		}
	}
	if v, ok := b.data[key]; ok {
		return b.uFn(v, out)
	}
	return ErrNotFound
}

func (b *Bucket) Set(key string, value interface{}) error {
	if b.tx == nil {
		return ErrReadOnly
	}

	var (
		val []byte
		err error
	)

	if val, err = b.mFn(value); err != nil {
		return err
	}

	b.tmp[key] = val
	b.tx.action(backend.ActionSet, b.name, key, val)

	return nil
}

func (b *Bucket) GetRaw(key string) []byte {
	if b.tx != nil {
		if v, ok := b.tmp[key]; ok {
			return copyBytes(v)
		}
	}

	return copyBytes(b.data[key])
}

func (b *Bucket) SetRaw(key string, val []byte) error {
	if b.tx == nil {
		return ErrReadOnly
	}

	val = copyBytes(val)
	b.tmp[key] = val
	b.tx.action(backend.ActionSet, b.name, key, val)

	return nil
}

func (b *Bucket) Delete(key string) error {
	if b.tx == nil {
		return ErrReadOnly
	}

	b.tmp[key] = nil
	b.tx.action(backend.ActionDelete, b.name, key, nil)

	return nil
}

func (b *Bucket) commit() {
	for k, v := range b.tmp {
		if v == nil {
			delete(b.data, k)
		} else {
			b.data[k] = v
		}
		delete(b.tmp, k)
	}
}

func (b *Bucket) rollback() {
	for k := range b.tmp {
		delete(b.tmp, k)
	}
}

// CreateBucket creates or returns a sub bucket by the specified name.
// TODO: implement sub buckets
func (b *Bucket) CreateBucket(name string) (*Bucket, error) { panic("n/i") }

// Bucket returns a sub bucket by the specified name, nil if it doesn't exist.
// TODO: implement sub buckets
func (b *Bucket) Bucket(name string) *Bucket { panic("n/i") }
