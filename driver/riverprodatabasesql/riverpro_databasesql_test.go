package riverprodatabasesql_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/stretchr/testify/require"
	"riverqueue.com/riverpro/driver"
	"riverqueue.com/riverpro/driver/riverprodatabasesql"
	"riverqueue.com/riverpro/driver/riverprodrivertest"
)

func TestDriverRiverProDatabaseSQLPgx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	stdPool := stdlib.OpenDBFromPool(riversharedtest.DBPool(ctx, t))
	t.Cleanup(func() { require.NoError(t, stdPool.Close()) })
	driverWithPool := riverprodatabasesql.New(stdPool)

	riverprodrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (driver.ProDriver[*sql.Tx], string) {
			t.Helper()
			return driverWithPool, riverdbtest.TestSchema(ctx, t, driverWithPool, opts)
		},
		func(ctx context.Context, t *testing.T) (driver.ProExecutor, driver.ProDriver[*sql.Tx]) {
			t.Helper()
			tx, schema := riverdbtest.TestTx(ctx, t, driverWithPool, nil)
			_, err := tx.ExecContext(ctx, "SET search_path TO '"+schema+"'")
			require.NoError(t, err)
			return driverWithPool.UnwrapProExecutor(tx), driverWithPool
		})
}
