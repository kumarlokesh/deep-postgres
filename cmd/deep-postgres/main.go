// deep-postgres is the entry point for the internal engine sandbox.
// At this stage it exists to confirm the module compiles; subsystem
// experiments will be wired in as each layer matures.
package main

import (
	"fmt"
	"os"

	"github.com/kumarlokesh/deep-postgres/internal/storage"
)

func main() {
	fmt.Println("deep-postgres: PostgreSQL internals sandbox")

	// Smoke-test the storage layer.
	pool := storage.NewBufferPool(128)
	id, err := pool.ReadBuffer(storage.MainTag(1, 0))
	if err != nil {
		fmt.Fprintln(os.Stderr, "buffer read failed:", err)
		os.Exit(1)
	}

	page, err := pool.GetPageForWrite(id)
	if err != nil {
		fmt.Fprintln(os.Stderr, "get page failed:", err)
		os.Exit(1)
	}

	tup := storage.NewHeapTuple(storage.FirstNormalTransactionId, 2, []byte("hello"))
	if _, err := page.InsertTuple(tup.ToBytes()); err != nil {
		fmt.Fprintln(os.Stderr, "insert failed:", err)
		os.Exit(1)
	}

	pool.UnpinBuffer(id)
	stats := pool.Stats()
	fmt.Printf("buffer pool: %d total, %d valid, %d dirty\n", stats.Total, stats.Valid, stats.Dirty)
	fmt.Println("ok")
}
