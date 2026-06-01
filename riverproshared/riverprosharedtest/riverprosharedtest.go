package riverprosharedtest

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"riverqueue.com/riverpro/driver/riverpropgxv5"
)

// TestTxPgxPro starts a test transaction that's rolled back automatically as
// the test case is cleaning itself up.
//
// This mirrors riverdbtest.TestTxPgx, but uses the River Pro pgx/v5 driver so
// the schema is migrated with River's main line plus the Pro line before the
// transaction is opened. The returned transaction has search_path set to the
// isolated test schema.
func TestTxPgxPro(ctx context.Context, tb testing.TB) pgx.Tx {
	tb.Helper()
	driver := riverpropgxv5.New(riversharedtest.DBPool(ctx, tb))
	tx, _ := riverdbtest.TestTxPgxDriver(ctx, tb, driver, nil)
	return tx
}

// WaitExpectTimeout tries to wait for a value and fails if one arrives. It is
// the inverse of riversharedtest.WaitOrTimeout and is useful for negative
// async assertions.
func WaitExpectTimeout[T any](t *testing.T, waitChan <-chan T) {
	t.Helper()
	select {
	case v := <-waitChan:
		t.Fatalf("expected timeout, got %#v", v)
	case <-time.After(100 * time.Millisecond):
	}
}
