package consumergroup

import (
	"sort"

	"github.com/funkygao/kazoo-go"
)

func retrievePartitionLeaders(partitions kazoo.PartitionList) (partitionLeaders, error) {
	pls := make(partitionLeaders, 0, len(partitions))
	for _, partition := range partitions {
		leader, err := partition.Leader()
		if err != nil {
			return nil, err
		}

		pl := partitionLeader{id: partition.ID, leader: leader, partition: partition}
		pls = append(pls, pl)
	}

	return pls, nil
}

// Divides a set of partitions between a set of consumers.
func dividePartitionsBetweenConsumers(consumers kazoo.ConsumergroupInstanceList,
	partitions partitionLeaders) map[string][]*kazoo.Partition {
	decision := make(map[string][]*kazoo.Partition)

	plen := len(partitions)
	clen := len(consumers)
	if clen == 0 || plen == 0 {
		return decision
	}

	sort.Sort(partitions)
	sort.Sort(consumers)

	n := plen / clen
	m := plen % clen
	p := 0
	for i, consumer := range consumers {
		first := p
		last := first + n
		if m > 0 && i < m {
			last++
		}
		if last > plen {
			last = plen
		}

		for _, pl := range partitions[first:last] {
			decision[consumer.ID] = append(decision[consumer.ID], pl.partition)
		}
		p = last
	}

	return decision
}
