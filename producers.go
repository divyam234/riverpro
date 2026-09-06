package riverpro

import (
	"context"
	"time"

	prodriver "github.com/divyam234/riverpro/driver"
)

// ProducerListOpts filters producers returned by ProducerList. An empty Queue
// returns producers for all queues.
type ProducerListOpts struct {
	Queue string
}

// Producer describes one River producer for one queue. A River client that
// consumes multiple queues has one producer row per queue with the same
// ClientID.
type Producer struct {
	ID         int64
	ClientID   string
	Queue      string
	MaxWorkers int
	PausedAt   *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// ProducerList returns registered River producers. Producer heartbeats update UpdatedAt.
func (c *Client[TTx]) ProducerList(ctx context.Context, opts *ProducerListOpts) ([]*Producer, error) {
	queue := ""
	if opts != nil {
		queue = opts.Queue
	}
	rows, err := c.proDriver.GetProExecutor().ProducerListByQueue(ctx, &prodriver.ProducerListByQueueParams{
		QueueName: queue,
		Schema:    c.config.Schema,
	})
	if err != nil {
		return nil, err
	}
	return producersFromDriver(rows)
}

// ProducerListTx is the transaction variant of ProducerList.
func (c *Client[TTx]) ProducerListTx(ctx context.Context, tx TTx, opts *ProducerListOpts) ([]*Producer, error) {
	queue := ""
	if opts != nil {
		queue = opts.Queue
	}
	rows, err := c.proDriver.UnwrapProExecutor(tx).ProducerListByQueue(ctx, &prodriver.ProducerListByQueueParams{
		QueueName: queue,
		Schema:    c.config.Schema,
	})
	if err != nil {
		return nil, err
	}
	return producersFromDriver(rows)
}

func producersFromDriver(rows []*prodriver.ProducerListByQueueResult) ([]*Producer, error) {
	producers := make([]*Producer, 0, len(rows))
	for _, row := range rows {
		if row == nil || row.Producer == nil {
			continue
		}
		producers = append(producers, &Producer{
			ID:         row.Producer.ID,
			ClientID:   row.Producer.ClientID,
			Queue:      row.Producer.QueueName,
			MaxWorkers: int(row.Producer.MaxWorkers),
			PausedAt:   row.Producer.PausedAt,
			CreatedAt:  row.Producer.CreatedAt,
			UpdatedAt:  row.Producer.UpdatedAt,
		})
	}
	return producers, nil
}
