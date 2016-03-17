package consumergroup

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/kazoo-go"
)

var (
	AlreadyClosing = errors.New("The consumer group is already shutting down.")
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
		return nil, errors.New("You need to provide at least one zookeeper node address")
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
			cg.Logf("FAILED to reset offsets of consumergroup: %s", err)
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
		cg.Logf("FAILED to check for existence of consumergroup: %s", err)
		_ = consumer.Close()
		_ = kz.Close()
		return nil, err
	} else if !exists {
		cg.Logf("Consumergroup `%s` does not yet exists, creating...", cg.group.Name)
		if err := cg.group.Create(); err != nil {
			cg.Logf("FAILED to create consumergroup in Zookeeper: %s", err)
			_ = consumer.Close()
			_ = kz.Close()
			return nil, err
		}
	}

	// Register itself with zookeeper
	if err := cg.instance.Register(topics); err != nil {
		cg.Logf("FAILED to register consumer instance: %s", err)
		return nil, err
	} else {
		cg.Logf("Consumer instance registered: %s", cg.instance.ID)
	}

	offsetConfig := OffsetManagerConfig{CommitInterval: config.Offsets.CommitInterval}
	cg.offsetManager = NewZookeeperOffsetManager(cg, &offsetConfig)

	go cg.topicListConsumer(topics)

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
		cg.Logf("CLOSING")

		defer cg.kazoo.Close()

		shutdownError = nil

		close(cg.stopper)
		cg.wg.Wait()

		if err := cg.offsetManager.Close(); err != nil {
			cg.Logf("FAILED closing the offset manager: %s", err)
		}

		if shutdownError = cg.instance.Deregister(); shutdownError != nil {
			cg.Logf("FAILED deregistering consumer instance: %s", shutdownError)
		} else {
			cg.Logf("Deregistered consumer instance %s", cg.instance.ID)
		}

		if shutdownError = cg.consumer.Close(); shutdownError != nil {
			cg.Logf("FAILED closing the Sarama client: %s", shutdownError)
		}

		close(cg.messages)
		close(cg.errors)
		cg.instance = nil
	})

	return shutdownError
}

func (cg *ConsumerGroup) Logf(format string, args ...interface{}) {
	var identifier string
	if cg.instance == nil {
		identifier = "(defunct)"
	} else {
		identifier = cg.instance.ID[len(cg.instance.ID)-12:]
	}
	sarama.Logger.Printf("[%s/%s] %s", cg.group.Name, identifier, fmt.Sprintf(format, args...))
}

func (cg *ConsumerGroup) CommitUpto(message *sarama.ConsumerMessage) error {
	return cg.offsetManager.MarkAsProcessed(message.Topic, message.Partition, message.Offset)
}

func (cg *ConsumerGroup) topicListConsumer(topics []string) {
	for {
		// each loop is a new rebalance process

		select {
		case <-cg.stopper:
			return
		default:
		}

		consumers, consumerChanges, err := cg.group.WatchInstances()
		if err != nil {
			cg.Logf("FAILED to get list of registered consumer instances: %s", err)
			return
		}

		cg.consumers = consumers
		cg.Logf("Currently registered consumers: %d", len(cg.consumers))

		topicConsumerStopper := make(chan struct{})
		topicChanges := make(chan struct{})

		for _, topic := range topics {
			cg.wg.Add(1)
			go cg.watchTopicChange(topic, topicConsumerStopper, topicChanges)
			go cg.topicConsumer(topic, cg.messages, cg.errors, topicConsumerStopper)
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
				cg.Logf("FAILED to get register status: %s", err)
			} else if !registered {
				err = cg.instance.Register(topics)
				if err != nil {
					cg.Logf("FAILED to register consumer instance: %s", err)
				} else {
					cg.Logf("Consumer instance registered: %s", cg.instance.ID)
				}
			}

			cg.Logf("Triggering rebalance due to consumer list change")
			close(topicConsumerStopper) // notify all topic consumers stop
			cg.wg.Wait()                // wait for all topic consumers finish

		case <-topicChanges:
			cg.Logf("Triggering rebalance due to topic partitions change")
			close(topicConsumerStopper) // notify all topic consumers stop
			cg.wg.Wait()                // wait for all topic consumers finish
		}
	}
}

// watchTopicChange watch partition changes on a topic.
func (cg *ConsumerGroup) watchTopicChange(topic string, stopper <-chan struct{}, topicChanges chan<- struct{}) {
	_, topicPartitionChanges, err := cg.kazoo.Topic(topic).WatchPartitions()
	if err != nil {
		cg.Logf("FAILED to get partitions of topic[%s]: %s", topic, err)
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

func (cg *ConsumerGroup) topicConsumer(topic string, messages chan<- *sarama.ConsumerMessage,
	errors chan<- *sarama.ConsumerError, stopper <-chan struct{}) {
	defer cg.wg.Done()

	select {
	case <-stopper:
		return
	default:
	}

	cg.Logf("%s :: Started topic consumer", topic)

	// Fetch a list of partition IDs
	partitions, err := cg.kazoo.Topic(topic).Partitions()
	if err != nil {
		cg.Logf("%s :: FAILED to get list of partitions: %s", topic, err)
		cg.errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: -1,
			Err:       err,
		}
		return
	}

	partitionLeaders, err := retrievePartitionLeaders(partitions)
	if err != nil {
		cg.Logf("%s :: FAILED to get leaders of partitions: %s", topic, err)
		cg.errors <- &sarama.ConsumerError{
			Topic:     topic,
			Partition: -1,
			Err:       err,
		}
		return
	}

	dividedPartitions := dividePartitionsBetweenConsumers(cg.consumers, partitionLeaders)
	myPartitions := dividedPartitions[cg.instance.ID]
	cg.Logf("%s :: Claiming %d of %d partitions", topic, len(myPartitions), len(partitionLeaders))

	// Consume all the assigned partitions
	var wg sync.WaitGroup
	for _, partition := range myPartitions {
		wg.Add(1)
		go cg.partitionConsumer(topic, partition.ID, messages, errors, &wg, stopper)
	}

	wg.Wait()
	cg.Logf("%s :: Stopped topic consumer", topic)
}

// Consumes a partition
func (cg *ConsumerGroup) partitionConsumer(topic string, partition int32, messages chan<- *sarama.ConsumerMessage,
	errors chan<- *sarama.ConsumerError, wg *sync.WaitGroup, stopper <-chan struct{}) {
	defer wg.Done()

	select {
	case <-stopper:
		return
	default:
	}

	for maxRetries, tries := 3, 0; tries < maxRetries; tries++ {
		if err := cg.instance.ClaimPartition(topic, partition); err == nil {
			break
		} else if err == kazoo.ErrPartitionClaimedByOther && tries+1 < maxRetries {
			time.Sleep(1 * time.Second)
		} else {
			cg.Logf("%s/%d :: FAILED to claim the partition: %s", topic, partition, err)
			return
		}
	}
	defer cg.instance.ReleasePartition(topic, partition)

	nextOffset, err := cg.offsetManager.InitializePartition(topic, partition)
	if err != nil {
		cg.Logf("%s/%d :: FAILED to determine initial offset: %s", topic, partition, err)
		return
	}

	if nextOffset >= 0 {
		cg.Logf("%s/%d :: Partition consumer starting at offset %d", topic, partition, nextOffset)
	} else {
		nextOffset = cg.config.Offsets.Initial
		if nextOffset == sarama.OffsetOldest {
			cg.Logf("%s/%d :: Partition consumer starting at the oldest available offset", topic, partition)
		} else if nextOffset == sarama.OffsetNewest {
			cg.Logf("%s/%d :: Partition consumer listening for new messages only", topic, partition)
		}
	}

	consumer, err := cg.consumer.ConsumePartition(topic, partition, nextOffset)
	if err == sarama.ErrOffsetOutOfRange {
		cg.Logf("%s/%d :: Partition consumer offset out of Range", topic, partition)
		// if the offset is out of range, simplistically decide whether to use OffsetNewest or OffsetOldest
		// if the configuration specified offsetOldest, then switch to the oldest available offset, else
		// switch to the newest available offset.
		if cg.config.Offsets.Initial == sarama.OffsetOldest {
			nextOffset = sarama.OffsetOldest
			cg.Logf("%s/%d :: Partition consumer offset reset to oldest available offset", topic, partition)
		} else {
			// even when user specifies initial offset, it is reset to newest
			nextOffset = sarama.OffsetNewest
			cg.Logf("%s/%d :: Partition consumer offset reset to newest available offset", topic, partition)
		}
		// retry the consumePartition with the adjusted offset
		consumer, err = cg.consumer.ConsumePartition(topic, partition, nextOffset)
	}
	if err != nil {
		cg.Logf("%s/%d :: FAILED to start partition consumer: %s", topic, partition, err)
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
					continue partitionConsumerLoop
				}
			}
		}
	}

	cg.Logf("%s/%d :: Stopping partition consumer at offset %d", topic, partition, lastOffset)
	if err := cg.offsetManager.FinalizePartition(topic, partition, lastOffset, cg.config.Offsets.ProcessingTimeout); err != nil {
		cg.Logf("%s/%d :: %s", topic, partition, err)
	}
}
