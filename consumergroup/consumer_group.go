package consumergroup

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/coocood/freecache"
	"github.com/funkygao/kazoo-go"
	log "github.com/funkygao/log4go"
	"github.com/samuel/go-zookeeper/zk"
)

// The ConsumerGroup type holds all the information for a consumer that is part
// of a consumer group. Call JoinConsumerGroup to start a consumer.
//
// You must call Close() on a consumer group to avoid leaks, it may not be
// garbage-collected automatically when it passes out of scope.
type ConsumerGroup struct {
	config *Config

	consumer sarama.Consumer

	kazoo    *kazoo.Kazoo
	group    *kazoo.Consumergroup
	instance *kazoo.ConsumergroupInstance

	wg             sync.WaitGroup
	singleShutdown sync.Once

	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
	stopper  chan struct{}

	offsetManager OffsetManager
	cacher        *freecache.Cache
}

func JoinConsumerGroupRealIp(realIp string, name string, topics []string, zookeeper []string,
	config *Config) (cg *ConsumerGroup, err error) {
	if name == "" {
		return nil, sarama.ConfigurationError("Empty consumergroup name")
	}
	if len(topics) == 0 {
		return nil, sarama.ConfigurationError("No topics provided")
	}
	if len(zookeeper) == 0 {
		return nil, EmptyZkAddrs
	}

	if config == nil {
		config = NewConfig()
	}
	config.ClientID = name
	if err = config.Validate(); err != nil {
		return
	}

	var kz *kazoo.Kazoo
	if kz, err = kazoo.NewKazoo(zookeeper, config.Zookeeper); err != nil {
		return
	}

	group := kz.Consumergroup(name)
	if config.Offsets.ResetOffsets {
		err = group.ResetOffsets()
		if err != nil {
			kz.Close()
			return
		}
	}

	instance := group.NewInstanceRealIp(realIp)

	cg = &ConsumerGroup{
		config: config,

		kazoo:    kz,
		group:    group,
		instance: instance,

		messages: make(chan *sarama.ConsumerMessage, config.ChannelBufferSize),
		errors:   make(chan *sarama.ConsumerError, config.ChannelBufferSize),
		stopper:  make(chan struct{}),
	}
	if config.NoDup {
		cg.cacher = freecache.NewCache(1 << 20) // TODO
	}

	// Register consumer group in zookeeper
	if exists, err := cg.group.Exists(); err != nil {
		_ = kz.Close()
		return nil, err
	} else if !exists {
		log.Debug("[%s/%s] consumer group in zk creating...", cg.group.Name, cg.shortID())

		if err := cg.group.Create(); err != nil {
			_ = kz.Close()
			return nil, err
		}
	}

	// Register itself with zookeeper: consumers/{group}/ids/{instanceId}
	// This will lead to consumer group rebalance
	if err := cg.instance.Register(topics); err != nil {
		return nil, err
	} else {
		log.Debug("[%s/%s] cg instance registered in zk for %+v", cg.group.Name, cg.shortID(), topics)
	}

	// kafka connect
	brokers, err := cg.kazoo.BrokerList()
	if err != nil {
		return nil, err
	}

	if consumer, err := sarama.NewConsumer(brokers, cg.config.Config); err != nil {
		return nil, err
	} else {
		cg.consumer = consumer
	}

	offsetConfig := OffsetManagerConfig{CommitInterval: config.Offsets.CommitInterval}
	cg.offsetManager = NewZookeeperOffsetManager(cg, &offsetConfig)

	cg.wg.Add(1)
	go cg.consumeTopics(topics)

	return
}

// Connects to a consumer group, using Zookeeper for auto-discovery
func JoinConsumerGroup(name string, topics []string, zookeeper []string,
	config *Config) (cg *ConsumerGroup, err error) {
	return JoinConsumerGroupRealIp("", name, topics, zookeeper, config)
}

func (cg *ConsumerGroup) Name() string {
	return cg.group.Name
}

// Returns a channel that you can read to obtain events from Kafka to process.
func (cg *ConsumerGroup) Messages() <-chan *sarama.ConsumerMessage {
	return cg.messages
}

func (cg *ConsumerGroup) emitError(err error, topic string, partition int32) {
	select {
	case cg.errors <- &sarama.ConsumerError{
		Topic:     topic,
		Partition: partition,
		Err:       err,
	}:
	case <-cg.stopper:
	default:
		// ignore
	}

}

// Returns a channel that you can read to obtain errors from Kafka to process.
func (cg *ConsumerGroup) Errors() <-chan *sarama.ConsumerError {
	return cg.errors
}

func (cg *ConsumerGroup) Close() error {
	shutdownError := AlreadyClosing
	cg.singleShutdown.Do(func() {
		log.Debug("[%s/%s] closing...", cg.group.Name, cg.shortID())

		shutdownError = nil

		close(cg.stopper) // notify all sub-goroutines to stop
		cg.wg.Wait()      // collect all sub-goroutines

		if err := cg.offsetManager.Close(); err != nil {
			// e,g. Not all offsets were committed before shutdown was completed
			log.Error("[%s/%s] closing offset manager: %s", cg.group.Name, cg.shortID(), err)
		}

		if shutdownError = cg.instance.Deregister(); shutdownError != nil {
			log.Error("[%s/%s] de-register cg instance: %s", cg.group.Name, cg.shortID(), shutdownError)
		} else {
			log.Debug("[%s/%s] de-registered cg instance", cg.group.Name, cg.shortID())
		}

		if cg.consumer != nil {
			if shutdownError = cg.consumer.Close(); shutdownError != nil {
				log.Error("[%s/%s] closing Sarama consumer: %v", cg.group.Name, cg.shortID(), shutdownError)
			}
		}

		close(cg.messages)
		close(cg.errors)

		log.Debug("[%s/%s] closed", cg.group.Name, cg.shortID())

		cg.instance = nil
		cg.kazoo.Close()
		if cg.cacher != nil {
			// nothing? TODO gc it quickly
		}
	})

	return shutdownError
}

func (cg *ConsumerGroup) shortID() string {
	var identifier string
	if cg.instance == nil {
		identifier = "(defunct)"
	} else {
		identifier = cg.instance.ID[len(cg.instance.ID)-12:]
	}

	return identifier
}

func (cg *ConsumerGroup) CommitUpto(message *sarama.ConsumerMessage) error {
	return cg.offsetManager.MarkAsProcessed(message.Topic, message.Partition, message.Offset)
}

func (cg *ConsumerGroup) consumeTopics(topics []string) {
	defer cg.wg.Done()

	for {
		// each loop is a new rebalance process

		select {
		case <-cg.stopper:
			return
		default:
		}

		consumers, consumerChanges, err := cg.group.WatchInstances()
		if err != nil {
			cg.emitError(err, topics[0], -1)
			return
		}

		topicConsumerStopper := make(chan struct{})
		topicConsumerStopped := make(chan struct{})
		topicChanges := make(chan struct{})

		for _, topic := range topics {
			cg.wg.Add(2)
			go cg.watchTopicChange(topic, topicConsumerStopper, topicChanges)
			go cg.consumeTopic(topic, consumers, topicConsumerStopper, topicConsumerStopped)
		}

		select {
		case <-cg.stopper:
			close(topicConsumerStopper) // notify all topic consumers stop
			<-topicConsumerStopped      // await all topic consumers being stopped
			return

		case <-consumerChanges:
			// when zk session expires, we need to re-register ephemeral znode
			//
			// how to reproduce:
			// iptables -A  OUTPUT -p tcp -m tcp --dport 2181 -j DROP # add rule
			// after 30s
			// iptables -D  OUTPUT -p tcp -m tcp --dport 2181 -j      # rm rule
			registered, err := cg.instance.Registered()
			if err != nil {
				// e,g. session expires, zk: could not connect to a server
				log.Error("[%s/%s] %s", cg.group.Name, cg.shortID(), err)
				cg.emitError(err, topics[0], -1)
			} else if !registered {
				// might be caused by zk session timeout
				err = cg.instance.Register(topics)
				if err != nil {
					log.Error("[%s/%s] register cg instance for %+v: %s", cg.group.Name, cg.shortID(), topics, err)
					cg.emitError(err, topics[0], -1)
				} else {
					log.Info("[%s/%s] re-registered cg instance for %+v", cg.group.Name, cg.shortID(), topics)
				}
			}

			log.Debug("[%s/%s] rebalance due to %+v cg members change", cg.group.Name, cg.shortID(), topics)
			close(topicConsumerStopper) // notify all topic consumers stop
			<-topicConsumerStopped      // await all topic consumers being stopped

		case <-topicChanges:
			log.Debug("[%s/%s] rebalance due to topic %+v change",
				cg.group.Name, cg.shortID(), topics)
			close(topicConsumerStopper) // notify all topic consumers stop
			<-topicConsumerStopped      // await all topic consumers being stopped
		}
	}
}

// watchTopicChange watch partition changes on a topic.
func (cg *ConsumerGroup) watchTopicChange(topic string, stopper <-chan struct{}, topicChanges chan<- struct{}) {
	defer cg.wg.Done()

	_, topicPartitionChanges, err := cg.kazoo.Topic(topic).WatchPartitions()
	if err != nil {
		if err == zk.ErrNoNode {
			err = ErrInvalidTopic
		}
		log.Error("[%s/%s] topic[%s] watch partitions: %s", cg.group.Name, cg.shortID(), topic, err)
		cg.emitError(err, topic, -1)
		return
	}

	select {
	case <-cg.stopper:
		return

	case <-stopper:
		return

	case <-topicPartitionChanges:
		close(topicChanges)
	}
}

func (cg *ConsumerGroup) consumeTopic(topic string, consumers kazoo.ConsumergroupInstanceList,
	stopper <-chan struct{}, stopped chan<- struct{}) {
	defer func() {
		cg.wg.Done()
		close(stopped)
	}()

	select {
	case <-stopper:
		return
	case <-cg.stopper:
		return
	default:
	}

	partitions, err := cg.kazoo.Topic(topic).Partitions()
	if err != nil {
		if err == zk.ErrNoNode {
			err = ErrInvalidTopic
		}
		log.Error("[%s/%s] topic[%s] get partitions: %s", cg.group.Name, cg.shortID(), topic, err)
		cg.emitError(err, topic, -1)
		return
	}

	partitionLeaders, err := retrievePartitionLeaders(partitions)
	if err != nil {
		log.Error("[%s/%s] get leader broker of topic[%s] partitions: %s", cg.group.Name, cg.shortID(), topic, err)
		cg.emitError(err, topic, -1)
		return
	}

	decision := dividePartitionsBetweenConsumers(consumers, partitionLeaders)
	myPartitions := decision[cg.instance.ID] // TODO if myPartitions didn't change, needn't rebalance

	if len(myPartitions) == 0 {
		if !cg.config.PermitStandby {
			cg.emitError(ErrTooManyConsumers, topic, -1)
		} else {
			// wait for rebalance chance
			consumerIDs := make([]string, 0, len(consumers))
			partitionIDs := make([]int32, 0, len(partitionLeaders))
			for _, c := range consumers {
				consumerIDs = append(consumerIDs, c.ID)
			}
			for _, p := range partitionLeaders {
				partitionIDs = append(partitionIDs, p.id)
			}
			log.Trace("[%s/%s] topic[%s] will standby {C:%d/%+v, P:%+v}", cg.group.Name, cg.shortID(), topic,
				len(consumerIDs), consumerIDs, partitionIDs)
		}
	} else {
		log.Debug("[%s/%s] topic[%s] claiming %d of %d partitions", cg.group.Name, cg.shortID(),
			topic, len(myPartitions), len(partitionLeaders))

		var wg sync.WaitGroup
		for _, partition := range myPartitions {
			wg.Add(1)
			go cg.consumePartition(topic, partition.ID, &wg, stopper)
		}

		wg.Wait()
	}

	log.Debug("[%s/%s] stopped consuming topic[%s] %d partitions", cg.group.Name, cg.shortID(), topic, len(myPartitions))
}

func (cg *ConsumerGroup) consumePartition(topic string, partition int32, wg *sync.WaitGroup, stopper <-chan struct{}) {
	defer wg.Done()

	select {
	case <-stopper:
		return
	case <-cg.stopper:
		return
	default:
	}

	for maxRetries, tries := 3, 0; tries < maxRetries; tries++ {
		if err := cg.instance.ClaimPartition(topic, partition); err == nil {
			log.Debug("[%s/%s] %s/%d claimed owner", cg.group.Name, cg.shortID(), topic, partition)
			break
		} else if err == kazoo.ErrPartitionClaimedByOther && tries+1 < maxRetries {
			time.Sleep(1 * time.Second)
		} else {
			cg.emitError(err, topic, partition)
			log.Error("[%s/%s] claim %s/%d: %s", cg.group.Name, cg.shortID(), topic, partition, err)
			return
		}
	}
	defer cg.instance.ReleasePartition(topic, partition)

	nextOffset, err := cg.offsetManager.InitializePartition(topic, partition)
	if err != nil {
		log.Error("[%s/%s] %s/%d determine initial offset: %s", cg.group.Name, cg.shortID(), topic, partition, err)
		return
	}

	if nextOffset >= 0 {
		log.Debug("[%s/%s] %s/%d start offset: %d", cg.group.Name, cg.shortID(), topic, partition, nextOffset)
	} else {
		nextOffset = cg.config.Offsets.Initial
		if nextOffset == sarama.OffsetOldest {
			log.Debug("[%s/%s] %s/%d start offset: oldest", cg.group.Name, cg.shortID(), topic, partition)
		} else if nextOffset == sarama.OffsetNewest {
			log.Debug("[%s/%s] %s/%d start offset: newest", cg.group.Name, cg.shortID(), topic, partition)
		}
	}

	consumer, err := cg.consumer.ConsumePartition(topic, partition, nextOffset)
	if err == sarama.ErrOffsetOutOfRange {
		// if the offset is out of range, simplistically decide whether to use OffsetNewest or OffsetOldest
		// if the configuration specified offsetOldest, then switch to the oldest available offset, else
		// switch to the newest available offset.
		if cg.config.Offsets.Initial == sarama.OffsetOldest {
			log.Warn("[%s/%s] %s/%d O:%d %s, reset to oldest", cg.group.Name, cg.shortID(), topic, partition, nextOffset, err)

			nextOffset = sarama.OffsetOldest
		} else {
			// even when user specifies initial offset, it is reset to newest
			log.Warn("[%s/%s] %s/%d O:%d %s, reset to newest", cg.group.Name, cg.shortID(), topic, partition, nextOffset, err)

			nextOffset = sarama.OffsetNewest
		}

		// retry the consumePartition with the adjusted offset
		consumer, err = cg.consumer.ConsumePartition(topic, partition, nextOffset)
	}
	if err != nil {
		// FIXME err chan?
		// e,g. In the middle of a leadership election
		// e,g. Tried to send a message to a replica that is not the leader for some partition. Your metadata is out of date
		// e,g. Request was for a topic or partition that does not exist on this broker
		// e,g. dial tcp 10.209.18.65:11005: getsockopt: connection refused
		cg.emitError(err, topic, partition)
		log.Error("[%s/%s] %s/%d: %s", cg.group.Name, cg.shortID(), topic, partition, err)
		return
	}
	defer consumer.Close()

	err = nil
	lastOffset := nextOffset

partitionConsumerLoop:
	for {
		select {
		case <-stopper:
			break partitionConsumerLoop

		case <-cg.stopper:
			break partitionConsumerLoop

		case err := <-consumer.Errors():
			for {
				select {
				case cg.errors <- err:
					continue partitionConsumerLoop

				case <-stopper:
					break partitionConsumerLoop
				}
			}

		case message := <-consumer.Messages():
			for {
				select {
				case <-stopper:
					break partitionConsumerLoop

				case cg.messages <- message:
					lastOffset = message.Offset + 1
					cg.offsetManager.MarkAsConsumed(topic, partition, message.Offset)
					continue partitionConsumerLoop
				}
			}
		}
	}

	log.Debug("[%s/%s] %s/%d stopping at offset: %d", cg.group.Name, cg.shortID(), topic, partition, lastOffset)
	if err := cg.offsetManager.FinalizePartition(topic, partition, lastOffset, cg.config.Offsets.ProcessingTimeout); err != nil {
		log.Error("[%s/%s] %s/%d: %s", cg.group.Name, cg.shortID(), topic, partition, err)
	}
}
