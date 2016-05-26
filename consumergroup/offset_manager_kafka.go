package consumergroup

import (
	"time"

	"github.com/Shopify/sarama"
)

type kafkaOffsetManager struct {
	offsetManager sarama.PartitionOffsetManager
	x             sarama.OffsetManager
}

func NewKafkaOffsetManager() OffsetManager {
	return &kafkaOffsetManager{}

}

func (this *kafkaOffsetManager) InitializePartition(topic string, partition int32) (int64, error) {
	offset, _ := this.offsetManager.NextOffset()
	return offset, nil
	//this.x.ManagePartition(topic, partition)
}

func (this *kafkaOffsetManager) MarkAsProcessed(topic string, partition int32, offset int64) error {
	this.offsetManager.MarkOffset(offset, "")
	return nil
}

func (this *kafkaOffsetManager) MarkAsConsumed(topic string, partition int32, offset int64) error {
	return nil
}

func (this *kafkaOffsetManager) FinalizePartition(topic string, partition int32, lastOffset int64,
	timeout time.Duration) error {
	return nil
}

func (this *kafkaOffsetManager) Close() error {
	return this.offsetManager.Close()
}
