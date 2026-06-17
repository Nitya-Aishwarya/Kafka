# Chapter 1: What Kafka Really Is

Apache Kafka is a **distributed event streaming platform**, which means it is designed to collect, store, and deliver continuous streams of data between different applications or systems. In simple words, Kafka acts as a middle layer where one application can publish information about something that happened, and many other applications can read that information whenever they need it.

Kafka is often compared to a message queue, but internally Kafka is much closer to a **distributed append-only log**. A log is a structure where new records are always added at the end, and old records are not immediately removed after being read. This design is very important because it allows Kafka to store historical events and let consumers replay them later. The official Kafka design documentation also describes Kafka as being built around partitioned, replicated logs rather than a traditional queue model. ([Apache Kafka][1])

For example, imagine an online shopping application. When a user places an order, the Order Service creates an event called `OrderCreated`. Instead of directly calling Payment Service, Inventory Service, Email Service, and Analytics Service, the Order Service sends this event to Kafka.

```text
Order Service
     |
     v
Kafka Topic: orders
     |
     |---- Payment Service
     |---- Inventory Service
     |---- Email Service
     |---- Analytics Service
```

This means the Order Service does not need to know who is using the event. It only writes the event once to Kafka. Any service that is interested can read it independently.

Internally, Kafka stores this event inside a topic partition. The event is appended to the end of a log file and receives a unique position called an offset. The offset is not globally unique across Kafka, but it is unique inside one partition.

Example:

```text
orders-topic partition-0

Offset 0 -> OrderCreated(orderId=101)
Offset 1 -> OrderPaid(orderId=101)
Offset 2 -> OrderShipped(orderId=101)
```

The most important idea is this: Kafka does not delete the event just because one consumer has read it. If the Payment Service reads `OrderCreated`, the event still remains in Kafka. Later, the Analytics Service can also read the same event. Even tomorrow, if retention allows, another service can start from offset `0` and replay the same data.

This is why Kafka is powerful: it separates **data production** from **data consumption**.

# Chapter 2: Why Kafka Was Created

Kafka was created to solve a common problem in large software systems: many applications need to exchange data continuously, but direct communication between every application becomes complex and fragile.

Suppose an e-commerce application has these services:

```text
Order Service
Payment Service
Inventory Service
Shipping Service
Notification Service
Analytics Service
Fraud Detection Service
```

Without Kafka, the Order Service might directly call every other service when a new order is placed.

```text
Order Service -> Payment Service
Order Service -> Inventory Service
Order Service -> Shipping Service
Order Service -> Notification Service
Order Service -> Analytics Service
Order Service -> Fraud Detection Service
```

This creates several problems. First, the Order Service becomes tightly coupled with many other services. Second, if one service is slow or unavailable, the Order Service may fail or become slow. Third, adding a new service becomes difficult because the Order Service must be changed every time a new consumer of order data appears.

Kafka solves this by introducing an event-driven architecture.

Instead of saying, “Order Service, please call these six services,” Kafka allows the Order Service to say, “An order was created,” and publish that fact as an event.

Then each service decides what to do.

```text
OrderCreated event
     |
     v
Kafka
     |
     |---- Payment Service charges money
     |---- Inventory Service reserves stock
     |---- Notification Service sends email
     |---- Analytics Service updates dashboard
```

This is called **decoupling**.

The producer does not know the consumers, and the consumers do not know the producer directly. They only agree on the structure and meaning of the event.

# Chapter 3: Event, Record, or Message

In Kafka, the terms **event**, **record**, and **message** are often used interchangeably, but the word “event” is the most meaningful from a design perspective. An event means that something happened in the real world or inside a system.

Examples:

```text
UserRegistered
OrderCreated
PaymentCompleted
PasswordChanged
ProductViewed
TemperatureMeasured
```

A Kafka event usually contains a key, value, timestamp, headers, and metadata.

Example event:

```json
{
  "orderId": 101,
  "customerId": 501,
  "amount": 2500,
  "status": "CREATED",
  "createdAt": "2026-06-17T10:30:00"
}
```

Internally, Kafka does not understand this JSON as business data. Kafka stores bytes. Your producer application serializes the object into bytes before sending it, and the consumer deserializes the bytes back into an object after reading it.

So logically, you see this:

```text
OrderCreated(orderId=101)
```

But internally Kafka sees something closer to this:

```text
010101011010010101
```

This is why serialization is very important in Kafka. Common serialization formats include JSON, Avro, Protobuf, and String serialization.

# Chapter 4: Topic

A Kafka topic is a named stream of related events. You can think of a topic as a category, folder, or table-like logical name where similar events are stored.

For example:

```text
orders
payments
shipments
users
inventory
```

If an event is related to orders, it may go to the `orders` topic. If it is related to payments, it may go to the `payments` topic.

However, internally Kafka does not store data directly inside a topic as one single file. A topic is divided into partitions. The topic is the logical name, but the partition is the actual storage and parallelism unit.

Example:

```text
Topic: orders

Partition 0
Partition 1
Partition 2
```

If a producer sends an event to the `orders` topic, Kafka decides which partition should receive that event. The event is then appended to that partition’s log.

The official Kafka operations documentation notes that each partition log is placed in its own folder under Kafka’s log directory, using the topic name and partition id in the folder name. ([Apache Kafka][2])

Example internal storage:

```text
/kafka-logs/orders-0/
/kafka-logs/orders-1/
/kafka-logs/orders-2/
```

So when you say “I sent data to the orders topic,” internally Kafka actually stores that data inside one of the partition directories belonging to that topic.

# Chapter 5: Partition

A partition is an ordered, append-only sequence of records. It is the most important internal unit in Kafka because it controls ordering, scalability, storage, and parallelism.

Suppose the `orders` topic has three partitions:

```text
orders-0
orders-1
orders-2
```

Events may be distributed like this:

```text
orders-0:
Offset 0 -> OrderCreated(101)
Offset 1 -> OrderPaid(101)

orders-1:
Offset 0 -> OrderCreated(102)
Offset 1 -> OrderCancelled(102)

orders-2:
Offset 0 -> OrderCreated(103)
Offset 1 -> OrderShipped(103)
```

Each partition has its own offsets. This means offset `0` can exist in partition `0`, partition `1`, and partition `2`. Therefore, an offset only makes sense when combined with the topic and partition.

A complete message position is:

```text
topic + partition + offset
```

Example:

```text
orders, partition 1, offset 0
```

Kafka guarantees ordering only inside a single partition. If `OrderCreated(101)` and `OrderPaid(101)` are written to the same partition, consumers will read them in the same order. But if they are written to different partitions, Kafka does not guarantee which one will be read first globally.

This is why message keys are important.

If you use `orderId` as the key, Kafka will send all events for the same order to the same partition.

```text
Key = orderId 101

OrderCreated(101) -> partition 0
OrderPaid(101)    -> partition 0
OrderShipped(101) -> partition 0
```

This preserves order for that order.

# Chapter 6: Offset

An offset is the position number of a record inside a partition. Kafka assigns offsets sequentially as records are appended.

Example:

```text
Partition 0

Offset 0 -> A
Offset 1 -> B
Offset 2 -> C
Offset 3 -> D
```

The offset is not controlled by the producer or consumer. Kafka assigns it.

Consumers use offsets to remember how far they have read. For example, if a consumer has processed messages up to offset `2`, the next message it should read is offset `3`.

The important internal idea is that Kafka does not track consumption by deleting messages. Instead, it stores consumer progress separately as committed offsets. Confluent’s Kafka consumer design documentation explains that consumers pull data from brokers and use offsets to track their position in the log. ([Confluent Documentation][3])

Example:

```text
Consumer Group: payment-service
Topic: orders
Partition: 0
Committed Offset: 3
```

This means the payment service has already processed records before offset `3`, and it should continue from offset `3` or after, depending on commit semantics.

This design allows replay.

If you reset the offset back to `0`, the consumer can read old messages again.

```text
Before:
Consumer starts from offset 3

After reset:
Consumer starts from offset 0
```

This is extremely useful when fixing bugs, rebuilding databases, reprocessing analytics, or recovering lost downstream data.

# Chapter 7: Producer Internal Flow

A Kafka producer is the application that sends records to Kafka. But internally, sending a message is not just one simple network call. Several steps happen before the message reaches the broker.

The internal producer flow looks like this:

```text
Application object
     |
     v
Serializer
     |
     v
Partitioner
     |
     v
Record Accumulator
     |
     v
Sender Thread
     |
     v
Kafka Broker
```

Suppose your Java application creates this object:

```java
OrderCreated order = new OrderCreated(101, 501, 2500);
```

Kafka cannot directly store a Java object. So first, the serializer converts it into bytes.

Then the partitioner decides which partition should receive the record. If the record has a key, Kafka usually hashes the key and maps it to a partition. If the record does not have a key, Kafka may distribute records using sticky partitioning so that batches are efficient.

After partition selection, the record is placed into an in-memory buffer called the record accumulator. Kafka producers do not always send every record immediately. They batch records together to improve performance.

Example:

```text
Instead of sending:

Record 1 -> network call
Record 2 -> network call
Record 3 -> network call

Kafka sends:

Batch [Record 1, Record 2, Record 3] -> one network call
```

This batching is one reason Kafka is very fast.

# Chapter 8: Broker

A broker is a Kafka server. A Kafka cluster contains one or more brokers.

Example:

```text
Kafka Cluster

Broker 1
Broker 2
Broker 3
```

Each broker stores some partitions and serves read/write requests from producers and consumers.

For example:

```text
Broker 1 stores orders-0
Broker 2 stores orders-1
Broker 3 stores orders-2
```

If the topic has replication enabled, each partition will have copies on multiple brokers.

Example with replication factor 3:

```text
orders-0 leader  -> Broker 1
orders-0 replica -> Broker 2
orders-0 replica -> Broker 3
```

The broker that owns the leader replica handles reads and writes for that partition. Followers copy data from the leader.

# Chapter 9: Replication

Replication means Kafka stores copies of the same partition on multiple brokers so that data remains available even if one broker fails.

Suppose `orders-0` has replication factor `3`.

```text
orders-0

Leader: Broker 1
Follower: Broker 2
Follower: Broker 3
```

When a producer writes to `orders-0`, the write goes to the leader. The followers fetch the new data from the leader and store their own copies.

If Broker 1 fails, Kafka can elect one of the followers as the new leader.

```text
Before failure:
Broker 1 = leader

After failure:
Broker 2 = new leader
```

This is how Kafka provides fault tolerance. Kafka’s replication design is based on replicating the log for each topic partition across multiple servers, allowing failover when a server fails. ([Confluent Documentation][4])

# Chapter 10: ISR

ISR means **In-Sync Replicas**.

An in-sync replica is a replica that is sufficiently caught up with the leader. Kafka does not treat all replicas equally. A follower may exist, but if it is too far behind the leader, Kafka should not trust it for leader election.

Example:

```text
Partition: orders-0

Leader: Broker 1
Replica: Broker 2
Replica: Broker 3
```

If all replicas are caught up:

```text
ISR = Broker 1, Broker 2, Broker 3
```

If Broker 3 becomes slow and falls behind:

```text
ISR = Broker 1, Broker 2
```

Broker 3 still has a replica, but it is no longer considered in-sync.

This matters because Kafka prefers to elect a new leader from the ISR. If an out-of-sync replica became leader, it might not contain the latest committed messages, causing data loss.

# Chapter 11: Consumer

A Kafka consumer is an application that reads records from Kafka.

Unlike some messaging systems where the broker pushes messages to consumers, Kafka consumers pull messages from brokers. This means the consumer asks Kafka for data when it is ready.

Example:

```text
Consumer: Give me records from orders-0 after offset 50.
Broker: Here are records 51 to 100.
```

This pull model is important because it allows consumers to control their own speed. A fast consumer can request more data frequently. A slow consumer can request less data. Kafka does not force messages onto the consumer.

# Chapter 12: Consumer Group

A consumer group is a group of consumers that cooperate to read from a topic.

Suppose the `orders` topic has three partitions:

```text
orders-0
orders-1
orders-2
```

And the payment service has three consumer instances:

```text
payment-consumer-1
payment-consumer-2
payment-consumer-3
```

Kafka assigns partitions like this:

```text
payment-consumer-1 -> orders-0
payment-consumer-2 -> orders-1
payment-consumer-3 -> orders-2
```

Inside one consumer group, a partition is consumed by only one consumer at a time. This prevents duplicate processing within the same group.

But different consumer groups can read the same topic independently.

Example:

```text
Consumer Group: payment-service
Reads orders topic

Consumer Group: analytics-service
Also reads orders topic

Consumer Group: notification-service
Also reads orders topic
```

Each group has its own offsets. This means payment, analytics, and notification services can all process the same events at different speeds.

Kafka’s introduction documentation describes consumer groups as distributing partitions among consumers so each partition is consumed by exactly one consumer in the group. ([Apache Kafka][5])

# Chapter 13: Rebalancing

Rebalancing is the process of redistributing partitions among consumers in a consumer group.

Rebalancing happens when:

```text
A new consumer joins
A consumer leaves
A consumer crashes
Topic partitions increase
```

Example before rebalance:

```text
C1 -> P0
C2 -> P1
C3 -> P2
```

If C2 crashes:

```text
C1 -> P0, P1
C3 -> P2
```

Kafka automatically assigns C2’s partition to another active consumer.

This gives high availability, but rebalancing can temporarily pause consumption. That is why production Kafka systems carefully tune heartbeat intervals, session timeouts, and partition assignment strategies.

# Chapter 14: Why Kafka Is Fast Internally

Kafka is fast because its design matches how operating systems and disks work efficiently.

Kafka writes data sequentially. Sequential disk writes are much faster than random writes because the disk or storage device does not need to jump around to many locations. Kafka simply appends new records to the end of the partition log.

Kafka also uses batching. Sending one large batch is much more efficient than sending thousands of tiny messages separately.

Kafka also benefits from the operating system page cache. Recently written or recently read data may remain in memory, so consumers may read from memory even though Kafka stores data on disk.

Another important optimization is zero-copy transfer. Normally, data might be copied from disk to kernel memory, then to application memory, then back to kernel memory, then to the network. Kafka avoids unnecessary copying and allows the operating system to move data more directly from disk/page cache to the network.

This is why Kafka can provide both durability and high throughput.

# Chapter 15: Final Revision Definition

Kafka is a distributed, fault-tolerant, append-only event streaming system that stores records in partitioned logs, replicates those logs across brokers for availability, allows producers to write events at high throughput, and allows independent consumer groups to read and replay those events using offsets.

This definition is worth memorizing.

[1]: https://kafka.apache.org/43/design/design/?utm_source=chatgpt.com "Design | Apache Kafka"
[2]: https://kafka.apache.org/43/operations/basic-kafka-operations/?utm_source=chatgpt.com "Basic Kafka Operations | Apache Kafka"
[3]: https://docs.confluent.io/kafka/design/consumer-design.html?utm_source=chatgpt.com "Kafka Consumer Design: Consumers, Consumer Groups, and Offsets - Confluent"
[4]: https://docs.confluent.io/kafka/design/replication.html?utm_source=chatgpt.com "Kafka Replication | Confluent Documentation"
[5]: https://kafka.apache.org/24/getting-started/introduction/?utm_source=chatgpt.com "Introduction - Apache Kafka"
