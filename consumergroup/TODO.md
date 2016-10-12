# TODO

- [ ] avoid the cracker: keep sub->close->sub flow, leads to consumer group fails to work
- [ ] standby with upper limit
- [X] rebalance when a topic partitions change
- [ ] ZKSessionExpireListener
- [ ] What if kafka broker conn timeout
- [ ] consumer a non-exist topic
- [ ] what if N partitions, but some consumer instances died?
- [X] commit offset out of range [low, high]
- [ ] load balance for consumer instances
