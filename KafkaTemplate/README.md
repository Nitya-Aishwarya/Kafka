# KafkaTemplate in Spring Boot 

`KafkaTemplate` is the main Spring Kafka class used by a producer application to publish messages or events into Kafka topics. When we say that a Spring Boot service “sends a message to Kafka,” in most real projects what actually happens is that the service calls a method on `KafkaTemplate`, and `KafkaTemplate` internally delegates the work to the native Kafka producer. Therefore, `KafkaTemplate` should not be understood as Kafka itself, and it should not be confused with a broker, topic, partition, or consumer. It is a Spring abstraction that sits inside your application and provides a clean, reusable, Spring-managed way to send records to Kafka without forcing every service class to manually create a `KafkaProducer`, build `ProducerRecord` objects, handle callbacks, manage producer lifecycle, and repeat the same configuration code again and again.

Before `KafkaTemplate`, a developer using Kafka directly would have to create a `KafkaProducer` manually, provide bootstrap servers, serializers, retry properties, acknowledgement properties, batching properties, and then create a `ProducerRecord` every time a message needed to be sent. This approach works, but in large Spring Boot applications it becomes repetitive and harder to maintain. Spring solves this by using the same design pattern used in `JdbcTemplate`, `RestTemplate`, and `RedisTemplate`: it creates a template class that hides infrastructure complexity and lets application code focus on business logic. In simple words, `KafkaTemplate` lets your service say, “publish this event to this topic,” while Spring and Kafka handle producer creation, serialization, metadata lookup, partition routing, batching, retries, network communication, and acknowledgement handling behind the scenes.

A very simple usage looks like this:

```java
@Service
@RequiredArgsConstructor
public class MemberEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishMemberCreated(MemberCreatedEvent event) {
        kafkaTemplate.send(
                "member-rx-topic",
                event.getMemberId(),
                event
        );
    }
}
```

In this example, `"member-rx-topic"` is the Kafka topic, `event.getMemberId()` is the message key, and `event` is the message value. The key is very important because Kafka uses the key to decide the target partition. If all events for the same member use the same `memberId` as the key, then those events will usually go to the same partition, and Kafka guarantees ordering inside that partition. This means events like `MemberCreated`, `MemberUpdated`, and `MemberDeleted` for the same member can be processed in order by consumers.

A sample event class may look like this:

```java
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MemberCreatedEvent {

    private String memberId;
    private String firstName;
    private String lastName;
    private String email;
    private LocalDateTime createdAt;
}
```

When your service creates this object, it exists only as a Java object inside JVM memory. Kafka brokers cannot store Java objects directly. Kafka stores bytes. Therefore, before the message is actually sent to Kafka, the underlying Kafka producer serializes the key and value. The key may be converted using `StringSerializer`, and the value may be converted using Spring Kafka’s `JsonSerializer`. After serialization, the event becomes a sequence of bytes that can travel over the network and be stored inside Kafka’s partition log.

A typical `application.yml` configuration for `KafkaTemplate` looks like this:

```yaml
spring:
  kafka:
    bootstrap-servers:
      - broker1:9092
      - broker2:9092
      - broker3:9092

    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 2147483647
      batch-size: 65536
      compression-type: zstd

      properties:
        enable.idempotence: true
        linger.ms: 10
        max.in.flight.requests.per.connection: 5
```

If these properties are already present in `application.yml` or `application.properties`, then in most normal Spring Boot applications you do not need to manually create a separate Kafka producer configuration class. Spring Boot auto-configuration reads these properties and automatically creates the required `ProducerFactory` and `KafkaTemplate` beans. This is why you can directly inject `KafkaTemplate` into your service. A custom configuration class is usually needed only when you have multiple Kafka clusters, multiple templates with different serializers, special transaction requirements, custom producer interceptors, or advanced enterprise-specific settings.

Internally, the relationship looks like this:

```text
Your Service
    ↓
KafkaTemplate
    ↓
ProducerFactory
    ↓
KafkaProducer
    ↓
Kafka Broker
```

`KafkaTemplate` itself does not directly manage all low-level Kafka networking. It uses a `ProducerFactory`, usually `DefaultKafkaProducerFactory`, and that factory provides the actual Kafka producer. The native Kafka producer is the component that performs serialization, partition selection, metadata lookup, batching, retries, and network communication with the broker that owns the target partition leader.

When the following line is executed:

```java
kafkaTemplate.send("member-rx-topic", event.getMemberId(), event);
```

the first thing that happens conceptually is that Spring prepares a Kafka `ProducerRecord`. A `ProducerRecord` is like an envelope that contains the topic name, key, value, optional partition, headers, and timestamp. Then the underlying producer serializes the key and value. After that, Kafka decides which partition should receive the record. If the topic has three partitions and the key is `"M101"`, Kafka may calculate something conceptually like:

```text
hash("M101") % 3 = 2
```

So the record is assigned to Partition 2. The producer then checks its metadata cache to find the leader broker for Partition 2. If Partition 2 is led by Broker 3, the producer sends the record directly to Broker 3. It does not send the record to the bootstrap broker unless that broker happens to be the leader for the selected partition. This is important because `bootstrap.servers` is only used for initial cluster discovery, not as a permanent routing destination.

The complete internal flow can be remembered like this:

```text
Service calls kafkaTemplate.send()
        ↓
KafkaTemplate creates ProducerRecord
        ↓
ProducerFactory supplies KafkaProducer
        ↓
KafkaProducer serializes key and value
        ↓
Partitioner selects target partition
        ↓
Producer metadata identifies leader broker
        ↓
Record enters RecordAccumulator
        ↓
Sender thread sends batch to leader broker
        ↓
Leader appends record to partition log
        ↓
Followers replicate the record
        ↓
Broker sends acknowledgement
        ↓
CompletableFuture completes
```

Modern `KafkaTemplate.send()` returns a `CompletableFuture`, which allows you to handle success and failure asynchronously. This is important because Kafka sending is normally asynchronous. The call to `send()` does not always mean the message has already been fully written and replicated at that exact moment. Instead, it means the send operation has been initiated, and the future will complete later.

Example with success and failure handling:

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class MemberEventPublisher {

    private final KafkaTemplate<String, MemberCreatedEvent> kafkaTemplate;

    public void publishMemberCreated(MemberCreatedEvent event) {

        CompletableFuture<SendResult<String, MemberCreatedEvent>> future =
                kafkaTemplate.send(
                        "member-rx-topic",
                        event.getMemberId(),
                        event
                );

        future.whenComplete((result, exception) -> {
            if (exception == null) {
                log.info(
                        "Message sent successfully. topic={}, partition={}, offset={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset()
                );
            } else {
                log.error(
                        "Failed to send message for memberId={}",
                        event.getMemberId(),
                        exception
                );
            }
        });
    }
}
```

The `SendResult` contains the actual Kafka metadata after the message is written. For example, it can tell you that the message was stored in `member-rx-topic`, Partition `2`, Offset `1057`. This is useful for debugging, logging, tracing, and understanding where exactly Kafka placed your event.

Sometimes you may want to send Kafka headers along with the message. Headers are useful for metadata such as correlation IDs, trace IDs, event types, source system names, schema versions, or tenant IDs. These values are not part of the business payload itself, but they travel with the message and help consumers understand or trace it.

Example with headers:

```java
public void publishWithHeaders(MemberCreatedEvent event) {

    Message<MemberCreatedEvent> message =
            MessageBuilder
                    .withPayload(event)
                    .setHeader(KafkaHeaders.TOPIC, "member-rx-topic")
                    .setHeader(KafkaHeaders.KEY, event.getMemberId())
                    .setHeader("eventType", "MemberCreated")
                    .setHeader("correlationId", UUID.randomUUID().toString())
                    .setHeader("sourceSystem", "member-service")
                    .build();

    kafkaTemplate.send(message);
}
```

In real applications, it is usually better to hide KafkaTemplate inside a dedicated publisher class instead of using it directly everywhere. This keeps Kafka-specific logic in one place and keeps business services cleaner.

Example:

```java
@Component
@RequiredArgsConstructor
public class KafkaEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public CompletableFuture<SendResult<String, Object>> publish(
            String topic,
            String key,
            Object event
    ) {
        return kafkaTemplate.send(topic, key, event);
    }
}
```

Then your business service can use it like this:

```java
@Service
@RequiredArgsConstructor
public class MemberService {

    private final KafkaEventPublisher eventPublisher;

    public void createMember(CreateMemberRequest request) {

        MemberCreatedEvent event =
                MemberCreatedEvent.builder()
                        .memberId(request.getMemberId())
                        .firstName(request.getFirstName())
                        .lastName(request.getLastName())
                        .email(request.getEmail())
                        .createdAt(LocalDateTime.now())
                        .build();

        eventPublisher.publish(
                "member-rx-topic",
                event.getMemberId(),
                event
        );
    }
}
```

This style is cleaner because if tomorrow you want to add headers, logging, tracing, retry wrapping, or custom error handling, you can modify the publisher class rather than changing every service method.

The producer properties used by KafkaTemplate are very important. `acks=all` means the producer waits for the leader broker to confirm the write after the required in-sync replicas have acknowledged it. This gives stronger durability than `acks=1`, where only the leader acknowledges. `enable.idempotence=true` helps prevent duplicate writes when retries happen. For example, if the broker receives the message but the acknowledgement is lost, the producer may retry. Without idempotence, the same message could be written twice. With idempotence, Kafka uses producer IDs and sequence numbers internally to detect duplicates. `linger.ms=10` allows the producer to wait a few milliseconds to collect more records into the same batch, improving throughput. `batch-size=65536` controls how large a batch can become. `compression-type=zstd` compresses batches so that less data travels over the network and less disk space is used.

A matching `application.properties` version would look like this:

```properties
spring.kafka.bootstrap-servers=broker1:9092,broker2:9092,broker3:9092

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer

spring.kafka.producer.acks=all
spring.kafka.producer.retries=2147483647
spring.kafka.producer.batch-size=65536
spring.kafka.producer.compression-type=zstd

spring.kafka.producer.properties.enable.idempotence=true
spring.kafka.producer.properties.linger.ms=10
spring.kafka.producer.properties.max.in.flight.requests.per.connection=5
```

A controller example may look like this:

```java
@RestController
@RequiredArgsConstructor
@RequestMapping("/members")
public class MemberController {

    private final MemberService memberService;

    @PostMapping
    public ResponseEntity<String> createMember(
            @RequestBody CreateMemberRequest request
    ) {
        memberService.createMember(request);
        return ResponseEntity.accepted()
                .body("Member creation event published");
    }
}
```

Request DTO:

```java
@Data
public class CreateMemberRequest {

    private String memberId;
    private String firstName;
    private String lastName;
    private String email;
}
```

The full application flow is now easy to understand. A REST request enters the controller. The controller calls the service. The service creates a domain event. The service calls the publisher. The publisher calls `KafkaTemplate`. `KafkaTemplate` delegates to the native Kafka producer. The producer serializes the record, selects a partition, finds the leader broker, batches the record, sends it to Kafka, waits for acknowledgement based on producer configuration, and completes the future with success or failure.

So the final mental model is this: `KafkaTemplate` is the Spring-managed gateway into Kafka’s producer pipeline. It lets business code publish events using simple method calls, while the underlying Kafka producer performs the real distributed-system work. When you call `kafkaTemplate.send()`, you are starting a chain that moves from Java object to serialized bytes, from topic to partition, from partition to leader broker, from leader broker to replicated Kafka log, and finally from Kafka acknowledgement back to your application through a `CompletableFuture`.
