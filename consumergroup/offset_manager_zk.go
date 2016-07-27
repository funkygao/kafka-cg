package consumergroup

import (
	"fmt"
	"sync"
	"time"

	log "github.com/funkygao/log4go"
)

type (
	topicOffsets    map[int32]*partitionOffsetTracker
	offsetsMap      map[string]topicOffsets
	offsetCommitter func(int64) error
)

type partitionOffsetTracker struct {
	l    sync.Mutex
	done chan struct{}

	waitingForOffset               int64
	highestMarkedAsProcessedOffset int64
	lastConsumedOffset             int64
	lastCommittedOffset            int64
}

type zookeeperOffsetManager struct {
	config  *OffsetManagerConfig
	l       sync.RWMutex
	offsets offsetsMap
	cg      *ConsumerGroup

	closing, closed chan struct{}
}

// NewZookeeperOffsetManager returns an offset manager that uses Zookeeper
// to store offsets.
func NewZookeeperOffsetManager(cg *ConsumerGroup, config *OffsetManagerConfig) OffsetManager {
	if config == nil {
		config = NewOffsetManagerConfig()
	}

	zom := &zookeeperOffsetManager{
		config:  config,
		cg:      cg,
		offsets: make(offsetsMap),
		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}

	go zom.offsetCommitter()

	return zom
}

func (zom *zookeeperOffsetManager) InitializePartition(topic string, partition int32) (int64, error) {
	zom.l.Lock()
	defer zom.l.Unlock()

	if zom.offsets[topic] == nil {
		zom.offsets[topic] = make(topicOffsets)
	}

	// if not found in zk, nextOffset will be -1
	nextOffset, err := zom.cg.group.FetchOffset(topic, partition)
	if err != nil {
		return 0, err
	}

	zom.offsets[topic][partition] = &partitionOffsetTracker{
		highestMarkedAsProcessedOffset: nextOffset - 1,
		lastCommittedOffset:            nextOffset - 1,
		lastConsumedOffset:             nextOffset - 1,
		done:                           make(chan struct{}),
	}

	return nextOffset, nil
}

func (zom *zookeeperOffsetManager) FinalizePartition(topic string, partition int32, lastOffset int64, timeout time.Duration) error {
	zom.l.RLock()
	tracker := zom.offsets[topic][partition]
	zom.l.RUnlock()

	if lastOffset >= 0 {
		if lastOffset-tracker.highestMarkedAsProcessedOffset > 0 {
			if !tracker.waitForOffset(lastOffset, timeout) {
				log.Debug("[%s/%s] %s/%d TIMEOUT %ds waiting for offset %d. Last committed offset: %d", zom.cg.group.Name, zom.cg.shortID(),
					topic, partition, timeout/time.Second, lastOffset,
					tracker.lastCommittedOffset)
			}
		}

		if err := zom.commitOffset(topic, partition, tracker); err != nil && err != NoOffsetToCommit {
			return fmt.Errorf("FAILED to commit offset %d to Zookeeper. Last committed offset: %d %v", tracker.highestMarkedAsProcessedOffset, tracker.lastCommittedOffset, err)
		}
	}

	zom.l.Lock()
	delete(zom.offsets[topic], partition)
	zom.l.Unlock()

	return nil
}

func (zom *zookeeperOffsetManager) MarkAsProcessed(topic string, partition int32, offset int64) error {
	zom.l.RLock()
	if p, ok := zom.offsets[topic][partition]; ok {
		zom.l.RUnlock()
		return p.markAsProcessed(offset)
	} else {
		zom.l.RUnlock()
		return TopicPartitionNotFound
	}
}

func (zom *zookeeperOffsetManager) MarkAsConsumed(topic string, partition int32, offset int64) error {
	zom.l.RLock()
	if p, ok := zom.offsets[topic][partition]; ok {
		zom.l.RUnlock()
		p.markAsConsumed(offset)
		return nil
	} else {
		zom.l.RUnlock()
		return TopicPartitionNotFound
	}
}

func (zom *zookeeperOffsetManager) Close() error {
	close(zom.closing)
	<-zom.closed

	zom.l.Lock()
	defer zom.l.Unlock()

	var closeError error
	for _, partitionOffsets := range zom.offsets {
		if len(partitionOffsets) > 0 {
			closeError = UncleanClose
		}
	}

	return closeError
}

func (zom *zookeeperOffsetManager) offsetCommitter() {
	commitTicker := time.NewTicker(zom.config.CommitInterval)
	defer commitTicker.Stop()

	for {
		select {
		case <-zom.closing:
			close(zom.closed)
			return

		case <-commitTicker.C:
			zom.commitOffsets()
		}
	}
}

func (zom *zookeeperOffsetManager) commitOffsets() {
	zom.l.RLock()
	defer zom.l.RUnlock()

	for topic, partitionOffsets := range zom.offsets {
		for partition, offsetTracker := range partitionOffsets {
			zom.commitOffset(topic, partition, offsetTracker)
		}
	}
}

func (zom *zookeeperOffsetManager) commitOffset(topic string, partition int32, tracker *partitionOffsetTracker) error {
	err := tracker.commit(func(offset int64) error {
		if offset >= 0 {
			return zom.cg.group.CommitOffset(topic, partition, offset+1)
		} else {
			return nil
		}
	})

	if err != nil && err != NoOffsetToCommit {
		log.Debug("[%s/%s] %s/%d commit offset %d: %s", zom.cg.group.Name, zom.cg.shortID(),
			topic, partition, tracker.highestMarkedAsProcessedOffset, err)
	}

	return err
}

// MarkAsProcessed marks the provided offset as highest processed offset if
// it's higehr than any previous offset it has received.
func (pot *partitionOffsetTracker) markAsProcessed(offset int64) error {
	pot.l.Lock()

	if offset > pot.lastConsumedOffset+1 {
		// last consumed msg offset=5, but client wants to commit offset=9
		pot.l.Unlock()

		return OffsetTooLarge
	} else if offset > pot.highestMarkedAsProcessedOffset {
		pot.highestMarkedAsProcessedOffset = offset
		if pot.waitingForOffset == pot.highestMarkedAsProcessedOffset {
			close(pot.done)
		}

		pot.l.Unlock()

		return nil
	} else {
		pot.l.Unlock()

		// client already committed offset=5, but now, it wants to commit offset=3
		return OffsetBackwardsError
	}
}

func (pot *partitionOffsetTracker) markAsConsumed(offset int64) {
	pot.l.Lock()
	pot.lastConsumedOffset = offset // markAsProcessed is using this var, so lock is required
	pot.l.Unlock()
}

// Commit calls a committer function if the highest processed offset is out
// of sync with the last committed offset.
func (pot *partitionOffsetTracker) commit(committer offsetCommitter) error {
	pot.l.Lock()
	defer pot.l.Unlock()

	if pot.highestMarkedAsProcessedOffset > pot.lastCommittedOffset {
		if err := committer(pot.highestMarkedAsProcessedOffset); err != nil {
			return err
		}

		pot.lastCommittedOffset = pot.highestMarkedAsProcessedOffset
		return nil
	} else {
		return NoOffsetToCommit
	}
}

func (pot *partitionOffsetTracker) waitForOffset(offset int64, timeout time.Duration) bool {
	pot.l.Lock()
	if offset > pot.highestMarkedAsProcessedOffset {
		pot.waitingForOffset = offset
		pot.l.Unlock()
		select {
		case <-pot.done:
			return true
		case <-time.After(timeout):
			return false
		}
	} else {
		pot.l.Unlock()
		return true
	}
}
