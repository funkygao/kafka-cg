package consumergroup

import (
	"time"
)

// OffsetManager is the main interface consumergroup requires to manage offsets of the consumergroup.
type OffsetManager interface {

	// InitializePartition is called when the consumergroup is starting to consume a
	// partition. It should return the last processed offset for this partition. Note:
	// the same partition can be initialized multiple times during a single run of a
	// consumer group due to other consumer instances coming online and offline.
	InitializePartition(topic string, partition int32) (int64, error)

	// MarkAsProcessed tells the offset manager that a certain message has been successfully
	// processed by the consumer, and should be committed. The implementation does not have
	// to store this offset right away, but should return true if it intends to do this at
	// some point.
	//
	// Offsets should generally be increasing if the consumer
	// processes events serially, but this cannot be guaranteed if the consumer does any
	// asynchronous processing. This can be handled in various ways, e.g. by only accepting
	// offsets that are higehr than the offsets seen before for the same partition.
	MarkAsProcessed(topic string, partition int32, offset int64) error

	MarkAsConsumed(topic string, partition int32, offset int64) error

	// FinalizePartition is called when the consumergroup is done consuming a
	// partition. In this method, the offset manager can flush any remaining offsets to its
	// backend store. It should return an error if it was not able to commit the offset.
	// Note: it's possible that the consumergroup instance will start to consume the same
	// partition again after this function is called.
	FinalizePartition(topic string, partition int32, lastOffset int64, timeout time.Duration) error

	// Close is called when the consumergroup is shutting down. In normal circumstances, all
	// offsets are committed because FinalizePartition is called for all the running partition
	// consumers. You may want to check for this to be true, and try to commit any outstanding
	// offsets. If this doesn't succeed, it should return an error.
	Close() error
}

// OffsetManagerConfig holds configuration setting son how the offset manager should behave.
type OffsetManagerConfig struct {
	CommitInterval time.Duration // Interval between offset flushes to the backend store.
	VerboseLogging bool          // Whether to enable verbose logging.
}

// NewOffsetManagerConfig returns a new OffsetManagerConfig with sane defaults.
func NewOffsetManagerConfig() *OffsetManagerConfig {
	return &OffsetManagerConfig{
		CommitInterval: 10 * time.Second,
	}
}
