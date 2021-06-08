/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.flink.core;

import static org.apache.flink.table.api.Expressions.$;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.scheme.proto.test.Scheme;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.Optionals;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import lombok.Data;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.junit.jupiter.api.Test;

class FlinkDataOperatorTest {

  private static final String MODEL =
      "{\n"
          + "  entities: {\n"
          + "    test {\n"
          + "      attributes {\n"
          + "        data: { scheme: \"string\" }\n"
          + "      }\n"
          + "    }\n"
          + "  }\n"
          + "\n"
          + "  attributeFamilies: {\n"
          + "    test_storage_stream {\n"
          + "      entity: test\n"
          + "      attributes: [ data ]\n"
          + "      storage: \"inmem:///test_inmem\"\n"
          + "      type: primary\n"
          + "      access: commit-log\n"
          + "    }\n"
          + "  }\n"
          + "\n"
          + "}\n";

  @Data
  public static class Result {
    String attribute;
    long latest;
  }

  @Test
  void testCreateOperator() {
    final Repository repository = Repository.ofTest(ConfigFactory.parseString(MODEL));
    repository.getOrCreateOperator(FlinkDataOperator.class);
  }

  @Test
  void testComplexSchema() throws Exception {
    final Repository repository = Repository.ofTest(ConfigFactory.load("test-proto.conf"));
    final FlinkDataOperator operator = repository.getOrCreateOperator(FlinkDataOperator.class);
    final StreamTableEnvironment tableEnvironment = operator.getTableEnvironment();
    operator.getExecutionEnvironment().setParallelism(3);

    final List<AttributeDescriptor<?>> attributes =
        Arrays.asList(
            repository.getEntity("gateway").getAttribute("status"),
            repository.getEntity("gateway").getAttribute("users"));

    operator.registerCommitLogTable(
        "gateway",
        attributes,
        ChangelogMode.insertOnly(),
        FlinkDataOperator.newCommitLogOptions().withShutdownFinishedSources(true).build());

    final CheckedThread thread =
        new CheckedThread() {

          @Override
          public void go() {
            final Random random = new Random();
            final TestWriter writer = new TestWriter(repository);
            writer.writeStatus(
                String.format("key_%s", random.nextInt(100)),
                Instant.now(),
                Scheme.Status.newBuilder()
                    .setConnected(true)
                    .setLastContact(1000L + random.nextInt(10000))
                    .build());
            writer.writeStatus(
                String.format("key_%s", random.nextInt(100)),
                Instant.ofEpochMilli(Watermarks.MAX_WATERMARK),
                Scheme.Status.newBuilder()
                    .setConnected(true)
                    .setLastContact(1000L + random.nextInt(10000))
                    .build());
            writer.writeUsers(
                String.format("key_%s", random.nextInt(100)),
                Instant.MAX,
                Scheme.Users.newBuilder()
                    .addUser(String.format("test_user_%s", random.nextInt(15)))
                    .addUser(String.format("test_user_%s", random.nextInt(15)))
                    .addUser(String.format("test_user_%s", random.nextInt(15)))
                    .addUser(String.format("test_user_%s", random.nextInt(15)))
                    .build());
          }
        };

    thread.start();

    final Table gateway = operator.getTable("gateway");

    System.out.printf("%n======= Print Schema =========%n%n");
    gateway.printSchema();

    tableEnvironment
        .from("gateway")
        .addColumns($("status").flatten())
        .select($("status$lastContact"));
    final Table result =
        tableEnvironment.sqlQuery(
            "SELECT attribute, MAX(status.lastContact) latest\n"
                + "FROM gateway\n"
                + "GROUP BY attribute");

    System.out.printf("%n======= EXPLAIN =========%n%n");
    System.out.println(result.explain());

    System.out.printf("%n======= Print Result Schema =========%n%n");
    result.printSchema();

    tableEnvironment
        .toRetractStream(result, Result.class)
        .addSink(
            new SinkFunction<Tuple2<Boolean, Result>>() {
              @Override
              public void invoke(Tuple2<Boolean, Result> value, Context context) throws Exception {
                System.out.println("======== " + value);
              }
            });

    result.printSchema();

    System.out.printf("%n======= Execute Query =========%n%n");

    operator.execute("test");
  }

  public static class TestWriter {

    private final Repository repository;
    private final DirectDataOperator direct;

    public TestWriter(Repository repository) {
      this.repository = repository;
      this.direct = repository.getOrCreateOperator(DirectDataOperator.class);
    }

    public void writeStatus(String key, Instant timestamp, Scheme.Status status) {
      final EntityDescriptor entity = repository.getEntity("gateway");
      final AttributeDescriptor<Scheme.Status> attribute = entity.getAttribute("status");
      try (final OnlineAttributeWriter writer = Optionals.get(direct.getWriter(attribute))) {
        writer.write(
            StreamElement.upsert(
                entity,
                attribute,
                UUID.randomUUID().toString(),
                key,
                attribute.getName(),
                timestamp.toEpochMilli(),
                attribute.getValueSerializer().serialize(status)),
            CommitCallback.noop());
      }
    }

    public void writeUsers(String key, Instant timestamp, Scheme.Users users) {
      final EntityDescriptor entity = repository.getEntity("gateway");
      final AttributeDescriptor<Scheme.Users> attribute = entity.getAttribute("users");
      try (final OnlineAttributeWriter writer = Optionals.get(direct.getWriter(attribute))) {
        writer.write(
            StreamElement.upsert(
                entity,
                attribute,
                UUID.randomUUID().toString(),
                key,
                attribute.getName(),
                timestamp.toEpochMilli(),
                attribute.getValueSerializer().serialize(users)),
            CommitCallback.noop());
      }
    }
  }
}
