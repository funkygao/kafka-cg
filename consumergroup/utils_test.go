package consumergroup

import (
	"testing"

	"github.com/funkygao/kazoo-go"
)

func TestDividePartitionsBetweenConsumers(t *testing.T) {
	consumers := make(kazoo.ConsumergroupInstanceList, 0)
	consumers = append(consumers, &kazoo.ConsumergroupInstance{ID: "1"})
	consumers = append(consumers, &kazoo.ConsumergroupInstance{ID: "2"})

	partitions := partitionLeaders{
		partitionLeader{
			id:        0,
			leader:    0,
			partition: &kazoo.Partition{ID: 0},
		},
		partitionLeader{
			id:        1,
			leader:    0,
			partition: &kazoo.Partition{ID: 1},
		},
		partitionLeader{
			id:        2,
			leader:    0,
			partition: &kazoo.Partition{ID: 2},
		},
	}
	result := dividePartitionsBetweenConsumers(consumers, partitions)
	for consumerId, topicPartitionInfo := range result {
		t.Logf("consumer:%s", consumerId)
		for _, tp := range topicPartitionInfo {
			t.Logf("%+v", tp)
		}
	}
}
