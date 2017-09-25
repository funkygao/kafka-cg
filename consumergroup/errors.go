package consumergroup

import (
	"errors"
)

var (
	EmptyZkAddrs           = errors.New("You need to provide at least one zookeeper node address")
	AlreadyClosing         = errors.New("The consumer group is already shutting down.")
	ConfigErrorOffset      = errors.New("Offsets.Initial should be sarama.OffsetOldest or sarama.OffsetNewest")
	UncleanClose           = errors.New("Not all offsets were committed before shutdown was completed")
	TopicPartitionNotFound = errors.New("Never consumed this topic/partition")
	OffsetBackwardsError   = errors.New("Offset to be committed is smaller than highest processed offset")
	NoOffsetToCommit       = errors.New("No offsets to commit")
	OffsetTooLarge         = errors.New("Offset to be committed is larger than highest consumed offset")
	ErrTooManyConsumers    = errors.New("Consumers more than active partitions")
	ErrInvalidTopic        = errors.New("Invalid topic")
	ErrConsumerConflict    = errors.New("One group can only consume one topic")
	ErrConnBroken          = errors.New("Kafka connection broken")
	ErrKafkaDead           = errors.New("Kakfa brokers all dead")
)
