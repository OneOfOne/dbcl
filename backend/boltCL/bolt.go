package boltCL

import (
	"log"
	"os"

	"github.com/OneOfOne/dbcl"
	"github.com/OneOfOne/dbcl/backend"
	"github.com/boltdb/bolt"
)

func NewDBCL(opts *dbcl.Options, path string, mode os.FileMode, boltOpts *bolt.Options) (*dbcl.DBCL, error) {
	be, err := New(path, mode, boltOpts)
	if err != nil {
		return nil, err
	}

	return dbcl.New(opts, be)
}

func New(path string, mode os.FileMode, options *bolt.Options) (backend.Backend, error) {
	db, err := bolt.Open(path, mode, options)
	if err != nil {
		return nil, err
	}
	return &boltBE{db}, nil
}

type boltBE struct {
	*bolt.DB
}

func (db *boltBE) LoadAll(fn backend.LoadFn) error {
	return db.DB.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			bname := string(name)
			return b.ForEach(func(k []byte, v []byte) error {
				return fn(bname, string(k), v)
			})
		})
	})
}

func (db *boltBE) Update(fn backend.UpdateFn) error {
	return db.DB.Update(func(tx *bolt.Tx) error {
		return fn(applyFn(tx))
	})
}

func (db *boltBE) Close() error {
	return db.DB.Close()
}

func (db *boltBE) Raw() interface{} { return db.DB }

func applyFn(tx *bolt.Tx) backend.ApplyFn {
	return func(a *backend.Action) (err error) {
		switch a.Type {
		case backend.ActionCreateBucket:
			_, err = tx.CreateBucketIfNotExists([]byte(a.Bucket))
		case backend.ActionDeleteBucket:
			err = tx.DeleteBucket([]byte(a.Bucket))
		case backend.ActionSet:
			var b *bolt.Bucket
			if b, err = tx.CreateBucketIfNotExists([]byte(a.Bucket)); err == nil {
				err = b.Put([]byte(a.Key), a.Value)
			}
		case backend.ActionDelete:
			err = tx.Bucket([]byte(a.Bucket)).Delete([]byte(a.Key))
		default:
			log.Panicf("invalid action: %+v", a)
		}
		return
	}
}
