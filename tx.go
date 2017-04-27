package dbcl

import (
	"fmt"
	"sort"

	"github.com/OneOfOne/dbcl/backend"
)

func (db *DBCL) getTx(bnames []string, ro bool) (*Tx, error) {
	tx, _ := db.txPool.Get().(*Tx)
	tx.ro = ro
	if err := tx.init(bnames); err != nil {
		db.putTx(tx)
		return nil, err
	}
	return tx, nil
}

func (db *DBCL) putTx(tx *Tx) {
	tx.actions = tx.actions[:0]
	db.txPool.Put(tx)
}

// Tx represents a database transaction.
type Tx struct {
	db      *DBCL
	buckets map[string]*Bucket
	actions []backend.Action
	ro      bool
}

// Get is shorthand for tx.Bucket(bucket).Get(key, out).
func (tx *Tx) Get(bucket, key string, out interface{}) error {
	return tx.Bucket(bucket).Get(key, out)
}

// Set is shorthand for tx.Bucket(bucket).Set(key, v).
func (tx *Tx) Set(bucket, key string, v interface{}) error {
	return tx.Bucket(bucket).Set(key, v)
}

// GetRaw is shorthand for tx.Bucket(bucket).GetRaw(key).
func (tx *Tx) GetRaw(bucket, key string) []byte {
	return tx.Bucket(bucket).GetRaw(key)
}

// SetRaw is shorthand for tx.Bucket(bucket).Set(key, v).
func (tx *Tx) SetRaw(bucket, key string, v []byte) error {
	return tx.Bucket(bucket).SetRaw(key, v)
}

// ForEach loops over all the buckets contained in *this* transaction.
func (tx *Tx) ForEach(fn func(name string, b *Bucket) error) error {
	for n, b := range tx.buckets {
		if err := fn(n, b); err != nil {
			return err
		}
	}
	return nil
}

// ForEachSorted loops over all the buckets contained in *this* transaction, sorted or reverse sorted by name.
func (tx *Tx) ForEachSorted(fn func(name string, b *Bucket) error, reverse bool) error {
	for _, name := range tx.KeysSorted(reverse) {
		if err := fn(name, tx.buckets[name]); err != nil {
			return err
		}
	}
	return nil
}

// Keys returns the names of all the buckets in *this* transaction.
func (tx *Tx) Keys() []string {
	keys := make([]string, 0, len(tx.buckets))
	for n := range tx.buckets {
		keys = append(keys, n)
	}
	return keys
}

// KeysSorted returns the names of all the buckets in *this* transaction, sorted or reverse sorted by name.
func (tx *Tx) KeysSorted(rev bool) []string {
	keys := tx.Keys()
	if rev {
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	} else {
		sort.Sort(sort.StringSlice(keys))
	}
	return keys
}

// Bucket will return a bucket with the specified name.
// If the bucket isn't already a part of this transaction, it will be locked and added to it.
// If the transaction is readonly, it will return nil if it doesn't exist, otherwise it gets created.
func (tx *Tx) Bucket(name string) *Bucket {
	b, ok := tx.buckets[name]
	if ok {
		return b
	}

	if tx.ro {
		return nil
	}

	tx.db.m.Lock()
	if b = tx.db.buckets[name]; b == nil {
		tx.action(backend.ActionCreateBucket, name, "", nil)
		b = newBucket(name, tx.db.opts.DefaultMarshalFn, tx.db.opts.DefaultUnmarshalFn)
		b.m.Lock()
		tx.db.buckets[name] = b
	}
	tx.db.m.Unlock()
	tx.buckets[name] = b

	return b
}

// SetBucket creates or updates a bucket with the specified Marshal/Unmarshal funcs,
// fallsback to the default ones if set to nil.
// The bucket will be locked to this transaction.
func (tx *Tx) SetBucket(name string, mFn MarshalFn, uFn UnmarshalFn) *Bucket {
	if mFn == nil {
		mFn = tx.db.opts.DefaultMarshalFn
	}
	if uFn == nil {
		uFn = tx.db.opts.DefaultUnmarshalFn
	}

	b, ok := tx.buckets[name]
	if ok {
		goto RET
	}

	tx.db.m.Lock()
	if b = tx.db.buckets[name]; b == nil {
		tx.action(backend.ActionCreateBucket, name, "", nil)
		b = newBucket(name, mFn, uFn)
		tx.db.buckets[name] = b
	}
	b.m.Lock()
	tx.db.m.Unlock()
	b.tx = tx
	tx.buckets[name] = b

RET:
	b.mFn, b.uFn = mFn, uFn
	return b
}

// DeleteBucket deletes a bucket.
func (tx *Tx) DeleteBucket(name string) error {
	if tx.ro {
		return ErrReadOnly
	}
	tx.action(backend.ActionDeleteBucket, name, "", nil)
	delete(tx.buckets, name)
	return nil
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
	if be := tx.db.be; be != nil {
		if err := be.Update(func(apply backend.ApplyFn) error {
			for i := range tx.actions {
				a := &tx.actions[i]
				if err := apply(a); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			tx.rollback()
			return err
		}
	}

	for i := range tx.actions {
		a := &tx.actions[i]
		switch a.Type {
		case backend.ActionCreateBucket: // this is needed in case the bucket gets deleted then re-added later.
			tx.db.m.Lock()
			tx.db.buckets[a.Bucket] = tx.buckets[a.Bucket]
			tx.db.m.Unlock()
		case backend.ActionDeleteBucket:
			tx.db.m.Lock()
			delete(tx.db.buckets, a.Bucket)
			tx.db.m.Unlock()
		}
	}

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
		for i := range tx.actions {
			a := &tx.actions[i]
			switch a.Type {
			case backend.ActionCreateBucket:
				tx.db.m.Lock()
				delete(tx.db.buckets, a.Bucket)
				tx.db.m.Unlock()
			}
		}
		for n, b := range tx.buckets {
			delete(tx.buckets, n)
			b.rollback()
			b.tx = nil
			b.m.Unlock()
		}
	}
}
