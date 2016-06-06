package consumergroup

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/kazoo-go"
	log "github.com/funkygao/log4go"
)

// The ConsumerGroup type holds all the information for a consumer that is part
// of a consumer group. Call JoinConsumerGroup to start a consumer.
type ConsumerGroup struct {
	config *Config

	consumer sarama.Consumer

	kazoo     *kazoo.Kazoo
	group     *kazoo.Consumergroup
	instance  *kazoo.ConsumergroupInstance
	consumers kazoo.ConsumergroupInstanceList

	wg             sync.WaitGroup
	singleShutdown sync.Once

	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
	stopper  chan struct{}

	offsetManager OffsetManager
}

// Connects to a consumer group, using Zookeeper for auto-discovery
func JoinConsumerGroup(name string, topics []string, zookeeper []string,
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

	var brokers []string
	brokers, err = kz.BrokerList()
	if err != nil {
		kz.Close()
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

	var consumer sarama.Consumer
	if consumer, err = sarama.NewConsumer(brokers, config.Config); err != nil {
		kz.Close()
		return
	}

	instance := group.NewInstance()
	cg = &ConsumerGroup{
		config:   config,
		consumer: consumer,

		kazoo:    kz,
		group:    group,
		instance: instance,

		messages: make(chan *sarama.ConsumerMessage, config.ChannelBufferSize),
		errors:   make(chan *sarama.ConsumerError, config.ChannelBufferSize),
		stopper:  make(chan struct{}),
	}

	// Register consumer group in zookeeper
	if exists, err := cg.group.Exists(); err != nil {
		_ = consumer.Close()
		_ = kz.Close()
		return nil, err
	} else if !exists {
		log.Debug("[%s/%s] consumer group in zk creating...", cg.group.Name, cg.shortID())

		if err := cg.group.Create(); err != nil {
			_ = consumer.Close()
			_ = kz.Close()
			return nil, err
		}
	}

	// Register itself with zookeeper: consumers/{group}/ids/{instanceId}
	// This will lead to consumer group rebalance
	if err := cg.instance.Register(topics); err != nil {
		return nil, err
	} else {
		log.Debug("[%s/%s] consumer instance registered in zk for %+v", cg.group.Name,
			cg.shortID(), topics)
	}

	offsetConfig := OffsetManagerConfig{CommitInterval: config.Offsets.CommitInterval}
	cg.offsetManager = NewZookeeperOffsetManager(cg, &offsetConfig)

	go cg.consumeTopics(topics)

	return
}

// Returns a channel that you can read to obtain events from Kafka to process.
func (cg *ConsumerGroup) Messages() <-chan *sarama.ConsumerMessage {
	return cg.messages
}

// Returns a channel that you can read to obtain errors from Kafka to process.
func (cg *ConsumerGroup) Errors() <-chan *sarama.ConsumerError {
	return cg.errors
}

func (cg *ConsumerGroup) Closed() bool {
	return cg.instance == nil
}

func (cg *ConsumerGroup) Close() error {
	shutdownError := AlreadyClosing
	cg.singleShutdown.Do(func() {
		defer cg.kazoo.Close()

		log.Debug("[%s/%s] closing...", cg.group.Name, cg.shortID())

		shutdownError = nil

		close(cg.stopper)
		cg.wg.Wait()

		if err := cg.offsetManager.Close(); err != nil {
			log.Error("[%s/%s] closing offset manager: %s", cg.group.Name, cg.shortID(), err)
		}

		if shutdownError = cg.instance.Deregister(); shutdownError != nil {
			log.Error("[%s/%s] de-register consumer instance: %s", cg.group.Name, cg.shortID(), shutdownError)
		} else {
			log.Debug("[%s/%s] de-registered consumer instance", cg.group.Name, cg.shortID())
		}

		if shutdownError = cg.consumer.Close(); shutdownError != nil {
			log.Error("[%s/%s] closing Sarama consumer: %v", cg.group.Name, cg.shortID(), shutdownError)
		}

		close(cg.messages)
		close(cg.errors)

		log.Debug("[%s/%s] closed", cg.group.Name, cg.shortID())

		cg.instance = nil
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
	for {
		// each loop is a new rebalance process

		select {
		case <-cg.stopper:
			return
		default:
		}

		consumers, consumerChanges, err := cg.group.WatchInstances()
		if err != nil {
			// FIXME write to err chan?
			log.Error("[%s/%s] watch consumer instances: %s", cg.group.Name, cg.shortID(), err)
			return
		}

		cg.consumers = consumers

		topicConsumerStopper := make(chan struct{})
		topicChanges := make(chan struct{})

		for _, topic := range topics {
			cg.wg.Add(1)
			go cg.watchTopicChange(topic, topicConsumerStopper, topicChanges)
			go cg.consumeTopic(topic, cg.messages, cg.errors, topicConsumerStopper)
		}

		select {
		case <-cg.stopper:
			close(topicConsumerStopper) // notify all topic consumers stop
			// cg.Close will call cg.wg.Wait()
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
				log.Error("[%s/%s] %s", cg.group.Name, cg.shortID(), err)
			} else if !registered {
				err = cg.instance.Register(topics)
				if err != nil {
					log.Error("[%s/%s] register consumer instance for %+v: %s",
						cg.group.Name, cg.shortID(), topics, err)
				} else {
					log.Warn("[%s/%s] re-registered consumer instance for %+v",
						cg.group.Name, cg.shortID(), topics)
				}
			}

			log.Debug("[%s/%s] rebalance due to %+v consumer list change",
				cg.group.Name, cg.shortID(), topics)
			close(topicConsumerStopper) // notify all topic consumers stop
			cg.wg.Wait()                // wait for all topic consumers finish

		case <-topicChanges:
			log.Debug("[%s/%s] rebalance due to topic %+v change",
				cg.group.Name, cg.shortID(), topics)
			close(topicConsumerStopper) // notify all topic consumers stop
			cg.wg.Wait()                // wait for all topic consumers finish
		}
	}
}

// watchTopicChange watch partition changes on a topic.
func (cg *ConsumerGroup) watchTopicChange(topic string, stopper <-chan struct{}, topicChanges chan<- struct{}) {
	_, topicPartitionChanges, err := cg.kazoo.Topic(topic).WatchPartitions()
	if err != nil {
		log.Error("[%s/%s] topic %s: %s", cg.group.Name, cg.shortID(), topic, err)
		// FIXME err chan?
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

func (cg *ConsumerGroup) consumeTopic(topic string, messages chan<- *sarama.ConsumerMessage,
	errors chan<- *sarama.ConsumerError, stopper <-chan struct{}) {
	defer cg.wg.Done()

	select {
	case <-stopper:
		return
	default:
	}

	log.Debug("[%s/%s] try consuming topic: %s", cg.group.Name, cg.shortID(), topic)

	partitions, err := cg.kazoo.Topic(topic).Partitions()
	if err != nil {
		log.Error("[%s/%s] get topic %s partitions: %s", cg.group.Name, cg.shortID(), topic, err)
		cg.errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: -1,
			Err:       err,
		}
		return
	}

	partitionLeaders, err := retrievePartitionLeaders(partitions)
	if err != nil {
		log.Error("[%s/%s] get leader broker of topic %s partitions: %s", cg.group.Name, cg.shortID(), topic, err)
		cg.errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: -1,
			Err:       err,
		}
		return
	}

	dividedPartitions := dividePartitionsBetweenConsumers(cg.consumers, partitionLeaders)
	myPartitions := dividedPartitions[cg.instance.ID]

	log.Debug("[%s/%s] topic %s claiming %d of %d partitions", cg.group.Name, cg.shortID(),
		topic, len(myPartitions), len(partitionLeaders))

	if len(myPartitions) == 0 {
		consumers := make([]string, 0, len(cg.consumers))
		partitions := make([]int32, 0, len(partitionLeaders))
		for _, c := range cg.consumers {
			consumers = append(consumers, c.ID)
		}
		for _, p := range partitionLeaders {
			partitions = append(partitions, p.id)
		}

		log.Trace("[%s/%s] topic %s will standby, {C:%+v, P:%+v}",
			cg.group.Name, cg.shortID(), topic, consumers, partitions)
	}

	// Consume all the assigned partitions
	var wg sync.WaitGroup
	for _, partition := range myPartitions {
		wg.Add(1)
		go cg.consumePartition(topic, partition.ID, messages, errors, &wg, stopper)
	}

	wg.Wait()
	log.Debug("[%s/%s] stopped consuming topic: %s", cg.group.Name, cg.shortID(), topic)
}

func (cg *ConsumerGroup) consumePartition(topic string, partition int32, messages chan<- *sarama.ConsumerMessage,
	errors chan<- *sarama.ConsumerError, wg *sync.WaitGroup, stopper <-chan struct{}) {
	defer wg.Done()

	select {
	case <-stopper:
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
			// FIXME err chan?
			log.Error("[%s/%s] claim %s/%d: %s", cg.group.Name, cg.shortID(), topic, partition, err)
			return
		}
	}
	defer func() {
		log.Debug("[%s/%s] %s/%d de-claiming owner", cg.group.Name, cg.shortID(), topic, partition)
		cg.instance.ReleasePartition(topic, partition)
	}()

	nextOffset, err := cg.offsetManager.InitializePartition(topic, partition)
	if err != nil {
		log.Error("[%s/%s] %s/%d determine initial offset: %s", cg.group.Name, cg.shortID(),
			topic, partition, err)
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
			log.Warn("[%s/%s] %s/%d O:%d %s, reset to oldest",
				cg.group.Name, cg.shortID(), topic, partition, nextOffset, err)

			nextOffset = sarama.OffsetOldest
		} else {
			// even when user specifies initial offset, it is reset to newest
			log.Warn("[%s/%s] %s/%d O:%d %s, reset to newest",
				cg.group.Name, cg.shortID(), topic, partition, nextOffset, err)

			nextOffset = sarama.OffsetNewest
		}

		// retry the consumePartition with the adjusted offset
		consumer, err = cg.consumer.ConsumePartition(topic, partition, nextOffset)
	}
	if err != nil {
		// FIXME err chan?
		log.Error("[%s/%s] %s/%d: %s", cg.group.Name, cg.shortID(), topic, partition, err)
		return
	}
	defer consumer.Close()

	err = nil
	var lastOffset int64 = -1 // aka unknown
partitionConsumerLoop:
	for {
		select {
		case <-stopper:
			break partitionConsumerLoop

		case err := <-consumer.Errors():
			for {
				select {
				case errors <- err:
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

				case messages <- message:
					lastOffset = message.Offset
					cg.offsetManager.MarkAsConsumed(topic, partition, lastOffset)
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
