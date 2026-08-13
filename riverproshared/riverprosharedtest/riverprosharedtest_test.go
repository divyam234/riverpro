package riverprosharedtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTestTxPgxPro(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tx := TestTxPgxPro(ctx, t)

	var tableCount int
	require.NoError(t, tx.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.tables
		WHERE table_schema = current_schema()
		  AND table_name IN (
			'river_job',
			'river_periodic_job',
			'river_producer',
			'river_job_sequence',
			'river_job_sequence_inbox',
			'river_workflow',
			'river_workflow_signal'
		  )
	`).Scan(&tableCount))
	require.Equal(t, 7, tableCount)
}

func TestWaitExpectTimeout(t *testing.T) {
	t.Parallel()

	ch := make(chan struct{})
	WaitExpectTimeout(t, ch)
}
