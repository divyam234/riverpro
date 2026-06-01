package riverprodatabasesql

import (
	"database/sql"

	"github.com/divyam234/riverpro/driver"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
)

type Driver = driver.Wrapper[*sql.Tx]
type Executor = driver.Executor
type ExecutorSubTx = driver.ExecutorTx
type ExecutorTx = driver.ExecutorTx

func New(dbPool *sql.DB) *Driver { return driver.NewWrapper[*sql.Tx](riverdatabasesql.New(dbPool)) }
