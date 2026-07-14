package riverpro

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProducerShutdownContextUsesIndependentBudget(t *testing.T) {
	parent, cancelParent := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelParent()

	ctx, cancel := producerShutdownContext(parent)
	defer cancel()

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.Greater(t, time.Until(deadline), 4*time.Second)

	<-parent.Done()
	require.NoError(t, ctx.Err(), "producer cleanup context must survive River's short retry deadline")
}
