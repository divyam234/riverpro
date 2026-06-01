package riverpropgxv5_test

import (
	"context"
	"testing"

	"github.com/divyam234/riverpro/driver"
	"github.com/divyam234/riverpro/driver/riverprodrivertest"
	"github.com/divyam234/riverpro/driver/riverpropgxv5"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/stretchr/testify/require"
)

func TestDriverRiverProPgxV5(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driverWithPool := riverpropgxv5.New(riversharedtest.DBPool(ctx, t))

	riverprodrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (driver.ProDriver[pgx.Tx], string) {
			t.Helper()
			return driverWithPool, riverdbtest.TestSchema(ctx, t, driverWithPool, opts)
		},
		func(ctx context.Context, t *testing.T) (driver.ProExecutor, driver.ProDriver[pgx.Tx]) {
			t.Helper()
			tx, _ := riverdbtest.TestTxPgxDriver(ctx, t, driverWithPool, nil)
			return driverWithPool.UnwrapProExecutor(tx), driverWithPool
		})
}

func TestNew(t *testing.T) {
	t.Parallel()

	d := riverpropgxv5.New(nil)
	require.NotNil(t, d)
	require.False(t, d.PoolIsSet())
}
