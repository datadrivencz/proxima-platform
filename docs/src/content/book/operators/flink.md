---
headless: false
draft: false
---

# Flink Data Operator

The Apache Flink data operator provides integrations with [Apache Flink](https://flink.apache.org/) to process your Proxima streams in a distributed, stateful, and highly scalable environment.

## Integration Overview

The Flink data operator provides a bridge between Proxima's `Repository` and Flink's `DataStream` API. This allows you to effortlessly consume Proxima `StreamElement`s natively inside your Flink streaming applications, without needing to manually define sources and sinks for underlying message brokers like Kafka or PubSub.

### Using Flink with Proxima

To use the Flink operator, you must add the appropriate dependency to your project:

```xml
<dependency>
  <groupId>cz.o2.proxima</groupId>
  <artifactId>proxima-flink-core</artifactId>
  <version>${proxima.version}</version>
</dependency>
```

### Basic DataStream Consumption

When using the Flink module, you can instantiate a `FlinkDataOperator` from your `Repository` and create a standard `DataStream` from your configured entity attributes.

```java
// 1. Initialize the Repository
Repository repo = Repository.of(ConfigFactory.load().resolve());

// 2. Obtain the Flink operator
FlinkDataOperator flinkOp = repo.getOrCreateOperator(FlinkDataOperator.class);

// 3. Create your Flink StreamExecutionEnvironment
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 4. Get the entity descriptors
EntityDescriptor gateway = repo.getEntity("gateway");
AttributeDescriptor<byte[]> armed = gateway.getAttribute("armed");

// 5. Create a DataStream from the commit log
DataStream<StreamElement> stream = flinkOp
    .getStreamElementSource(env)
    .forAttribute(armed)
    .build();

// 6. Process the stream using standard Flink operations
stream.map(element -> {
    System.out.println("Received: " + element.getKey());
    return element;
}).print();

env.execute("Proxima Flink Job");
```

## Supported Features

The Flink data operator currently supports:
- Translating Proxima attributes into `DataStream<StreamElement>`.
- Native integration with the underlying storage semantics.
- Full stream processing, windowing, and stateful operations via Flink's native APIs.

By utilizing the `FlinkDataOperator`, your data pipelines remain abstracted from the actual physical storage (whether it be Kafka, Google PubSub, etc.), providing extreme flexibility in deployment and testing.
