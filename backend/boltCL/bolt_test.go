package boltCL_test

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/OneOfOne/dbcl"
	"github.com/OneOfOne/dbcl/backend/boltCL"
	"github.com/boltdb/bolt"
)

func TestBackend(t *testing.T) {
	log.SetFlags(log.Lshortfile)
	f, err := ioutil.TempFile("", "dbcl_bolt")
	if err != nil {
		t.Fatal(err)
	}
	fp := f.Name()
	f.Close()
	defer os.Remove(fp)

	opts := &dbcl.Options{
		DefaultMarshalFn:   json.Marshal,
		DefaultUnmarshalFn: json.Unmarshal,
		BucketMapping: []dbcl.BucketMapping{
			{Name: "raw", MarshalFn: dbcl.MarshalRaw, UnmarshalFn: dbcl.UnmarshalRaw},
		},
	}

	db, err := boltCL.NewDBCL(opts, fp, 0755, &bolt.Options{Timeout: 1})

	if err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *dbcl.Tx) error {
		tx.Bucket("default").Set("struct", result{"hi", 100})
		tx.Bucket("raw").Set("string", "string")
		return nil
	}, "default", "raw"); err != nil {
		t.Fatal(err)
	}

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}

	if db, err = boltCL.NewDBCL(opts, fp, 0755, &bolt.Options{Timeout: 1}); err != nil {
		t.Fatal(err)

	}

	db.View(func(tx *dbcl.Tx) error {
		var r result
		tx.Bucket("default").Get("struct", &r)

		if r.N != "hi" || r.I != 100 {
			t.Errorf("got %#+v", r)
		}
		var v string
		tx.Bucket("raw").Get("string", &v)
		if v != "string" {
			t.Errorf("got %#+v", r)
		}
		return nil
	})

	if err = db.Close(); err != nil {
		t.Error(err)
		return
	}

}

type result struct {
	N string
	I int64
}
