package dbcl

import (
	"encoding/json"
	"errors"
	"log"
	"sync"

	"github.com/OneOfOne/dbcl/backend"
)

// Errors
var (
	ErrNoBuckets      = errors.New("no buckets specified")
	ErrBucketNotFound = errors.New("bucket not found")
	ErrClosed         = errors.New("db is closed")
	ErrNotFound       = errors.New("not found")
	ErrReadOnly       = errors.New("write operation on a ReadOnly Tx.")

	defaultOptions = &Options{
		DefaultMarshalFn:   json.Marshal,
		DefaultUnmarshalFn: json.Unmarshal,
	}
)

// Options sets options used by DBCL.
type Options struct {
	DefaultMarshalFn   MarshalFn
	DefaultUnmarshalFn UnmarshalFn
	BucketMapping      []BucketMapping
}

// BucketMapping allows using a custom Marshal/Unmarshal funcs for the specified bucket.
type BucketMapping struct {
	Name        string
	MarshalFn   MarshalFn
	UnmarshalFn UnmarshalFn
}

// DBCL aka Database Cache Layer
type DBCL struct {
	buckets map[string]*Bucket
	opts    *Options
	be      backend.Backend
	m       sync.RWMutex
	txPool  sync.Pool
}

// New returns a new DBCL, with optionally the specified Backend.
// If backend is nil, it will be a simple memory-only store.
// If opts is nil, it defaults to using json.Marshal/json.Unmarshal.
func New(opts *Options, be backend.Backend) (*DBCL, error) {
	if opts == nil {
		opts = defaultOptions
	}

	if opts.DefaultMarshalFn == nil {
		opts.DefaultMarshalFn = defaultOptions.DefaultMarshalFn
	}
	if opts.DefaultUnmarshalFn == nil {
		opts.DefaultUnmarshalFn = defaultOptions.DefaultUnmarshalFn
	}

	db := &DBCL{
		buckets: make(map[string]*Bucket),
		opts:    opts,
		be:      be,
	}

	if err := db.CreateBucketMapping(opts.BucketMapping...); err != nil {
		if be != nil {
			be.Close()
		}
		return nil, err
	}

	if be != nil {
		if err := be.LoadAll(db.loadAll); err != nil {
			be.Close()
			return nil, err
		}
	}

	db.txPool.New = func() interface{} { return &Tx{db: db} }

	return db, nil
}

// CreateBucketMapping allows creating new buckets with the specfied type mapping.
// Note: You must either call this or set Options.BucketMapping before trying to access those buckets for the first time.
func (db *DBCL) CreateBucketMapping(bms ...BucketMapping) error {
	if db.be == nil {
		goto SKIP
	}
	if err := db.be.Update(func(apply backend.ApplyFn) error {
		for i := range bms {
			if err := apply(&backend.Action{Type: backend.ActionCreateBucket, Bucket: bms[i].Name}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

SKIP:
	db.m.Lock()
	for _, bm := range bms {
		// handle nil
		if b := db.buckets[bm.Name]; b == nil {
			db.buckets[bm.Name] = newBucket(bm.Name, bm.MarshalFn, bm.UnmarshalFn)
		} else {
			b.mFn, b.uFn = bm.MarshalFn, bm.UnmarshalFn
		}

	}
	db.m.Unlock()
	return nil
}

func (db *DBCL) loadAll(bucket, key string, value []byte) error {
	b := db.buckets[bucket]
	if b == nil {
		b = &Bucket{name: bucket, mFn: db.opts.DefaultMarshalFn, uFn: db.opts.DefaultUnmarshalFn}
		db.buckets[bucket] = b
	}
	if b.data == nil {
		b.data = make(map[string][]byte)
	}
	b.data[key] = value
	return nil
}

func (db *DBCL) View(fn func(tx *Tx) error, bnames ...string) error {
	tx, err := db.getTx(bnames, true)
	if err != nil {
		return err
	}
	defer func() {
		tx.rollback()
		db.putTx(tx)
	}()

	return fn(tx)
}

func (db *DBCL) Update(fn func(tx *Tx) error, bnames ...string) (err error) {
	tx, err := db.getTx(bnames, false)
	if err != nil {
		return err
	}
	defer func() {
		if v := recover(); v != nil {
			log.Printf("panic (%T): %v", v, v)
			tx.rollback()
		}
		db.putTx(tx)
	}()

	if err := fn(tx); err != nil {
		tx.rollback()
		return err
	}

	return tx.commit()
}

func (db *DBCL) Get(bucket, key string, out interface{}) error {
	db.m.RLock()
	b := db.buckets[bucket]
	db.m.RUnlock()
	if b == nil {
		return ErrNotFound
	}

	b.m.RLock()
	err := b.Get(key, out)
	b.m.RUnlock()

	return err
}

func (db *DBCL) Set(bucket, key string, v interface{}) error {
	return db.Update(func(tx *Tx) error {
		return tx.Bucket(bucket).Set(key, v)
	}, bucket)
}

func (db *DBCL) Close() error {
	db.m.Lock()
	defer db.m.Unlock()
	if db.buckets == nil {
		return ErrClosed
	}
	db.buckets = nil

	if db.be != nil {
		return db.be.Close()
	}
	return nil
}

func (db *DBCL) Closed() bool {
	db.m.RLock()
	b := db.buckets == nil
	db.m.RUnlock()
	return b
}

func (db *DBCL) Buckets() []string {
	db.m.RLock()
	names := make([]string, 0, len(db.buckets))
	for n := range db.buckets {
		names = append(names, n)
	}
	db.m.RUnlock()
	return names
}
