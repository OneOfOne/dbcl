package dbcl

import (
	"fmt"
	"sync"

	"github.com/OneOfOne/dbcl/backend"
)

var txPool = sync.Pool{
	New: func() interface{} { return &Tx{} },
}

func getTx(db *DBCL, bnames []string, ro bool) (*Tx, error) {
	tx, _ := txPool.Get().(*Tx)
	tx.db, tx.ro = db, ro
	if err := tx.init(bnames); err != nil {
		putTx(tx)
		return nil, err
	}
	return tx, nil
}

func putTx(tx *Tx) {
	txPool.Put(tx)
}

type Tx struct {
	db        *DBCL
	buckets   map[string]*Bucket
	actions   []backend.Action
	ro        bool
	noBackend bool
}

func (tx *Tx) Bucket(name string) *Bucket {
	return tx.buckets[name]
}

func (tx *Tx) init(bnames []string) (err error) {
	tx.db.m.Lock()
	defer tx.db.m.Unlock()

	if tx.db.buckets == nil {
		return ErrClosed
	}

	if tx.buckets == nil {
		tx.buckets = make(map[string]*Bucket, len(tx.db.buckets)+1)
	}

	if len(bnames) == 0 {
		for n, b := range tx.db.buckets {
			if tx.ro {
				b.m.RLock()
			} else {
				b.m.Lock()
				b.tx = tx
			}
			tx.buckets[n] = b
		}
		return
	}

	for _, bname := range bnames {
		b := tx.db.buckets[bname]
		if b == nil {
			if tx.ro {
				for n := range tx.buckets {
					delete(tx.buckets, n)
				}
				return fmt.Errorf("bucket (%s) doesn't exist", bname)
			}
			b = newBucket(bname, tx.db.opts.DefaultMarshalFn, tx.db.opts.DefaultUnmarshalFn)
			tx.db.buckets[bname] = b
		}
		if tx.ro {
			b.m.RLock()
		} else {
			b.m.Lock()
			b.tx = tx
		}
		tx.buckets[bname] = b
	}

	return
}

func (tx *Tx) action(typ uint8, bucket, key string, value []byte) {
	if tx.db.be == nil {
		return
	}
	tx.actions = append(tx.actions, backend.Action{Bucket: bucket, Key: key, Value: value, Type: typ})
}

func (tx *Tx) commit() error {
	be := tx.db.be
	if be == nil {
		goto COMMIT_BUCKETS
	}

	if err := be.Update(func(apply backend.ApplyFn) error {
		for i := range tx.actions {
			if err := apply(&tx.actions[i]); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		tx.rollback()
		return err
	}

COMMIT_BUCKETS:
	for n, b := range tx.buckets {
		delete(tx.buckets, n)
		b.commit()
		b.tx = nil
		b.m.Unlock()
	}

	return nil
}

func (tx *Tx) rollback() {
	if tx.ro {
		for n, b := range tx.buckets {
			delete(tx.buckets, n)
			b.m.RUnlock()
		}
	} else {
		for n, b := range tx.buckets {
			delete(tx.buckets, n)
			b.rollback()
			b.tx = nil
			b.m.Unlock()
		}
	}
}
