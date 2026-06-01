package riverprodatabasesql

import (
	"database/sql"

	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"riverqueue.com/riverpro/driver"
)

type Driver = driver.Wrapper[*sql.Tx]
type Executor = driver.Executor
type ExecutorSubTx = driver.ExecutorTx
type ExecutorTx = driver.ExecutorTx

func New(dbPool *sql.DB) *Driver { return driver.NewWrapper[*sql.Tx](riverdatabasesql.New(dbPool)) }
