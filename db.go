package dbcl

import (
	"encoding/json"
	"errors"
	"log"
	"sync"

	"github.com/OneOfOne/dbcl/backend"
)

var (
	ErrNoBuckets = errors.New("no buckets specified")
	ErrClosed    = errors.New("db is closed")

	defaultOptions = &Options{
		DefaultMarshalFn:   json.Marshal,
		DefaultUnmarshalFn: json.Unmarshal,
	}
)

type Options struct {
	DefaultMarshalFn   MarshalFn
	DefaultUnmarshalFn UnmarshalFn
	BucketMapping      []BucketMapping
}

type BucketMapping struct {
	Name        string
	MarshalFn   MarshalFn
	UnmarshalFn UnmarshalFn
}

// DBCL aka DataBase Cache Layer
type DBCL struct {
	buckets map[string]*Bucket
	opts    *Options
	be      backend.Backend
	m       sync.RWMutex
}

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

	return db, nil
}

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
			db.buckets[bm.Name] = &Bucket{
				name: bm.Name,
				mFn:  bm.MarshalFn,
				uFn:  bm.UnmarshalFn,
			}
		} else {
			b.mFn, b.uFn = bm.MarshalFn, bm.UnmarshalFn
		}

	}
	db.m.Unlock()
	return nil
}

func (db *DBCL) DeleteBucket(name string) error {
	if db.be != nil {
		if err := db.be.Update(func(apply backend.ApplyFn) error {
			return apply(&backend.Action{Type: backend.ActionDeleteBucket, Bucket: name})
		}); err != nil {
			return err
		}
	}
	db.m.Lock()
	delete(db.buckets, name)
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
	tx, err := getTx(db, bnames, true)
	if err != nil {
		return err
	}
	defer func() {
		tx.rollback()
		putTx(tx)
	}()

	return fn(tx)
}

func (db *DBCL) Update(fn func(tx *Tx) error, bnames ...string) (err error) {
	log.Println(0)
	tx, err := getTx(db, bnames, false)
	if err != nil {
		return err
	}
	defer func() {
		// if v := recover(); v != nil {
		// 	log.Printf("panic (%T): %v", v, v)
		// 	tx.rollback()
		// }
		putTx(tx)
	}()

	if err := fn(tx); err != nil {
		tx.rollback()
		return err
	}

	return tx.commit()
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
