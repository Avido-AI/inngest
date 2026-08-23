package queue

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/stretchr/testify/require"
)

type batchEnqueueTestShard struct {
	*mockShardForIterator
	items []QueueItem
	ats   []time.Time
	errs  []error
}

func (s *batchEnqueueTestShard) EnqueueItemBatch(_ context.Context, items []QueueItem, ats []time.Time, _ EnqueueOpts) []error {
	s.items = items
	s.ats = ats
	return s.errs
}

func TestQueueProducerEnqueueBatchUsesProductionBatchEnqueuer(t *testing.T) {
	shard := &batchEnqueueTestShard{
		mockShardForIterator: &mockShardForIterator{name: "test"},
		errs: []error{
			nil,
			QueueItemExists("existing", nil),
		},
	}
	registry := mustSingleShardRegistry(t, shard)
	producer := NewProducer(registry)

	batch, ok := producer.(BatchEnqueuer)
	require.True(t, ok)

	firstAt := time.Now()
	secondAt := firstAt.Add(time.Minute)
	firstJobID := "first"
	secondJobID := "second"
	errs := batch.EnqueueBatch(context.Background(), []Item{
		{
			JobID: &firstJobID,
			Kind:  "test",
			Identifier: state.Identifier{
				WorkflowID: uuid.New(),
			},
		},
		{
			JobID: &secondJobID,
			Kind:  "test",
			Identifier: state.Identifier{
				WorkflowID: uuid.New(),
			},
		},
	}, []time.Time{firstAt, secondAt}, EnqueueOpts{})

	require.Len(t, errs, 2)
	require.NoError(t, errs[0])
	require.ErrorIs(t, errs[1], ErrQueueItemExists)
	require.Len(t, shard.items, 2)
	require.Equal(t, []time.Time{
		time.UnixMilli(firstAt.UnixMilli()),
		time.UnixMilli(secondAt.UnixMilli()),
	}, shard.ats)
	require.Equal(t, "first", shard.items[0].ID)
	require.Equal(t, "second", shard.items[1].ID)
}
