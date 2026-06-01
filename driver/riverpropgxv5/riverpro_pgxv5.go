package riverpropgxv5

import (
	"github.com/divyam234/riverpro/driver"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type Driver = driver.Wrapper[pgx.Tx]
type Executor = driver.Executor
type ExecutorTx = driver.ExecutorTx

func New(dbPool *pgxpool.Pool) *Driver { return driver.NewWrapper[pgx.Tx](riverpgxv5.New(dbPool)) }
