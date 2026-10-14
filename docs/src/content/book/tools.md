---
headless: false
draft: false
---

# Tools

The Proxima platform provides various tooling designed to help you interact with your data in real-time. This chapter describes the core tools available, including the `proxima-tools` Groovy console CLI.

## Proxima Groovy Console (`proxima-tools`)

`proxima-tools` provides an interactive Groovy console that loads your configuration and makes your entities readily available for reading and writing. This is an incredibly useful environment for debugging, manual intervention, exploring data streams, and managing your `Repository` instance.

### Running the Console

To run the console, you must execute the main class `cz.o2.proxima.tools.groovy.Console` with your compiled `Repository` configurations and storage dependencies on the classpath. Typically, this is done by building a shaded JAR of your Proxima project and running:

```bash
java -cp my-proxima-project-shaded.jar cz.o2.proxima.tools.groovy.Console
```

Once the shell loads, the environment initializes a `Repository` using the loaded configuration (`ConfigFactory.load()`) and exposes several bindings into your environment.

### Available Bindings

Within the shell, you will have access to an `env` object that automatically acts as a gateway to your entities and attributes. By default, properties map directly to the entities you've defined in your `reference.conf`.

#### Querying Entities

If your model defines an entity named `gateway` with attributes `armed` and `device.*`, you can interact with it directly:

```groovy
// Check if a key exists using the idiomatic `in` operator
'my-gateway-id' in env.gateway.armed
'my-device-id' in env.gateway.device
```

#### Reading Data

You can start reading streams or batch querying the data directly via the `env`:

```groovy
// Stream all data from the oldest available point and collect them into a list
def history = env.gateway.armed.streamFromOldest().collect()

// You can limit or reduce this stream to get the latest state
def state = env.gateway.armed.streamFromOldest().reduceToLatest().collect()
```

#### Writing Data

You can write data back to the entity (which translates into upserts on your underlying storage):

```groovy
// Write a new state for a specific key
env.gateway.armed.put("my-gateway-id", [state: true])

// Delete a specific wildcard attribute
env.gateway.device.delete("my-gateway-id", "device_123")
```

### SQL Support

The Proxima Groovy Console also integrates with Apache Calcite, providing an idiomatic `sql()` method. This allows you to evaluate full SQL queries against the active data in your repository.

```groovy
// Query your data using Calcite SQL syntax
env.sql("SELECT * FROM PROXIMA.GATEWAY WHERE key = 'gw'") { row ->
    println "Found key: ${row.key} with timestamp ${row.stamp}"
}

// Or execute a statement directly
def result = env.sql("SELECT count(*) FROM PROXIMA.GATEWAY")
```

The underlying schema maps your Proxima entities into SQL tables, leveraging your pre-configured `Repository` instance without any additional setup.
