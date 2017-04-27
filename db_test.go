package dbcl_test

import (
	"testing"

	"github.com/OneOfOne/dbcl"
)

func TestDB(t *testing.T) {
	db, _ := dbcl.New(nil, nil)
	err := db.Update(func(tx *dbcl.Tx) error {
		return tx.Bucket("test").Set("test", "test")
	}, "test")
	t.Log(err)
	var v string
	err = db.View(func(tx *dbcl.Tx) error {
		t.Logf("%#+v", tx.Bucket("test"))
		return tx.Bucket("test").Get("test", &v)
	}, "test")

	t.Logf("value: %v, %v", v, err)
}

