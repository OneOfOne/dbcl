# dbcl [![GoDoc](http://godoc.org/github.com/OneOfOne/dbcl?status.svg)](http://godoc.org/github.com/OneOfOne/dbcl) [![Build Status](https://travis-ci.org/OneOfOne/dbcl.svg?branch=master)](https://travis-ci.org/OneOfOne/dbcl)
--

dbcl a simple, efficient, typed memory caching layer.

## Install

	go get github.com/OneOfOne/dbcl

## Usage

```go
import (
	"github.com/OneOfOne/dbcl"
	"github.com/OneOfOne/backend/boltCL"
)

func main() {
	be, err := boltCL.New(path, mode, boltOpts)
	if err != nil {
		log.Fatal(err)
	}

	db, err := dbcl.New(&dbcl.Options{
		DefaultMarshalFn:   json.Marshal,
		DefaultUnmarshalFn: json.Unmarshal,
		BucketMapping: []dbcl.BucketMapping{
			{Name: "raw", MarshalFn: dbcl.MarshalRaw, UnmarshalFn: dbcl.UnmarshalRaw},
		},
	}, be)

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.Update(func(tx *dbcl.Tx) error {
		tx.Bucket("default").Set("struct", DataStruct{"hi", 100})
		tx.Bucket("raw").Set("string", "string")
		return nil
	}
}
```

## Benchmark
```bash

```

## License

Apache v2.0 (see [LICENSE](https://github.com/OneOfOne/dbcl/blob/master/LICENSE) file).

Copyright 2016-2016 Ahmed <[OneOfOne](https://github.com/OneOfOne/)> W.

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
