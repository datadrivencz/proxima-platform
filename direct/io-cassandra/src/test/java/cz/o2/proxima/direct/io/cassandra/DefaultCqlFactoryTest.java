/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.cassandra;

import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.AttributeDescriptorBase;
import cz.o2.proxima.core.repository.ConfigRepository;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.io.serialization.proto.Serialization.Cell;
import cz.o2.proxima.io.serialization.shaded.com.google.protobuf.ByteString;
import cz.o2.proxima.typesafe.config.Config;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

/** Test for default CQL factory. */
public class DefaultCqlFactoryTest {

  final Config cfg = ConfigFactory.defaultApplication();
  final Repository repo = ConfigRepository.Builder.ofTest(cfg).build();
  final AttributeDescriptorBase<?> attr;
  final AttributeDescriptorBase<?> attrWildcard;
  final EntityDescriptor entity;
  final PreparedStatement statement = mock(PreparedStatement.class);
  final CqlSession session = mock(CqlSession.class);

  CqlFactory factory;
  List<String> preparedStatement;

  public DefaultCqlFactoryTest() {
    this.attr =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dummy")
            .setName("myAttribute")
            .setSchemeUri(URI.create("bytes:///"))
            .build();
    this.attrWildcard =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dummy")
            .setName("device.*")
            .setSchemeUri(URI.create("bytes:///"))
            .build();
    this.entity =
        EntityDescriptor.newBuilder()
            .setName("dummy")
            .addAttribute(attr)
            .addAttribute(attrWildcard)
            .build();
  }

  @Before
  public void setup() {
    preparedStatement = new ArrayList<>();
    factory =
        new DefaultCqlFactory() {

          // store the generated statements for inspection

          @Override
          protected String createInsertStatement(StreamElement what) {
            preparedStatement.add(super.createInsertStatement(what));
            return preparedStatement.get(preparedStatement.size() - 1);
          }

          @Override
          protected String createDeleteStatement(StreamElement what) {
            preparedStatement.add(super.createDeleteStatement(what));
            return preparedStatement.get(preparedStatement.size() - 1);
          }

          @Override
          protected String createDeleteWildcardStatement(StreamElement what) {
            preparedStatement.add(super.createDeleteWildcardStatement(what));
            return preparedStatement.get(preparedStatement.size() - 1);
          }

          @Override
          protected String createListStatement(AttributeDescriptor<?> desc) {
            preparedStatement.add(super.createListStatement(desc));
            return preparedStatement.get(preparedStatement.size() - 1);
          }

          @Override
          protected String createGetStatement(String attribute, AttributeDescriptor<?> desc) {
            preparedStatement.add(super.createGetStatement(attribute, desc));
            return preparedStatement.get(preparedStatement.size() - 1);
          }

          @Override
          protected String createFetchTokenStatement() {
            preparedStatement.add(super.createFetchTokenStatement());
            return preparedStatement.get(preparedStatement.size() - 1);
          }

          @Override
          protected String createListEntitiesStatement() {
            preparedStatement.add(super.createListEntitiesStatement());
            return preparedStatement.get(preparedStatement.size() - 1);
          }
        };
    factory.setup(
        entity,
        URI.create("cassandra://wherever/my_table?data=my_col&primary=hgw"),
        StringConverter.getDefault());
  }

  @Test(expected = IllegalStateException.class)
  public void testSetupWithNoQuery() {
    factory.setup(
        entity, URI.create("cassandra://wherever/my_table"), StringConverter.getDefault());
  }

  @Test
  public void testSetupWithJustPrimary() {
    factory.setup(
        entity,
        URI.create("cassandra://wherever/my_table?primary=hgw"),
        StringConverter.getDefault());
    assertNotNull(factory);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetupWithNoPath() {
    factory.setup(entity, URI.create("cassandra://wherever/"), StringConverter.getDefault());
  }

  @Test
  public void testIngest() {
    long now = System.currentTimeMillis();
    StreamElement ingest =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            "myAttribute",
            now,
            "value".getBytes());
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", ByteBuffer.wrap("value".getBytes()), now * 1000L)).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    verify(statement).bind(eq("key"), eq(ByteBuffer.wrap("value".getBytes())), eq(now * 1000L));
    assertTrue(boundStatement.isPresent());
    assertSame(bound, boundStatement.get());
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "INSERT INTO my_table (hgw, my_attribute) VALUES (?, ?) USING TIMESTAMP ?",
        preparedStatement.get(0));
  }

  @Test
  public void testIngestWithTtl() {
    long now = System.currentTimeMillis();
    factory.setup(
        entity,
        URI.create("cassandra://wherever/my_table?data=my_col&primary=hgw&ttl=86400"),
        StringConverter.getDefault());
    StreamElement ingest =
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            "myAttribute",
            now,
            "value".getBytes());
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", ByteBuffer.wrap("value".getBytes()), now * 1000L)).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);
    when(bound.set(eq(1), any(), eq(ByteBuffer.class))).thenReturn(bound);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    verify(statement).bind(eq("key"), eq(ByteBuffer.wrap("value".getBytes())), eq(now * 1000L));
    assertTrue(boundStatement.isPresent());
    assertSame(bound, boundStatement.get());
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "INSERT INTO my_table (hgw, my_attribute) VALUES (?, ?) USING TIMESTAMP ?"
            + " AND TTL 86400",
        preparedStatement.get(0));
  }

  @Test
  public void testIngestWildcard() {
    long now = System.currentTimeMillis();
    StreamElement ingest =
        StreamElement.upsert(
            entity,
            attrWildcard,
            UUID.randomUUID().toString(),
            "key",
            "device.1",
            now,
            "value".getBytes());
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", "1", ByteBuffer.wrap("value".getBytes()), now * 1000L))
        .thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    verify(statement)
        .bind(
            eq("key"), eq("1"),
            eq(ByteBuffer.wrap("value".getBytes())), eq(now * 1000L));
    assertTrue(boundStatement.isPresent());
    assertSame(bound, boundStatement.get());
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "INSERT INTO my_table (hgw, device, my_col) VALUES (?, ?, ?) USING TIMESTAMP ?",
        preparedStatement.get(0));
  }

  @Test
  public void testIngestWildcardMultiDots() {
    long now = System.currentTimeMillis();
    StreamElement ingest =
        StreamElement.upsert(
            entity,
            attrWildcard,
            UUID.randomUUID().toString(),
            "key",
            "device.1.2",
            now,
            "value".getBytes());
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", "1.2", ByteBuffer.wrap("value".getBytes()), now * 1000L))
        .thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    verify(statement)
        .bind(
            eq("key"), eq("1.2"),
            eq(ByteBuffer.wrap("value".getBytes())), eq(now * 1000L));
    assertTrue(boundStatement.isPresent());
    assertSame(bound, boundStatement.get());
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "INSERT INTO my_table (hgw, device, my_col) VALUES (?, ?, ?) USING TIMESTAMP ?",
        preparedStatement.get(0));
  }

  @Test
  public void testIngestDeleteSimple() {
    long now = System.currentTimeMillis();
    StreamElement ingest =
        StreamElement.delete(entity, attr, UUID.randomUUID().toString(), "key", "myAttribute", now);
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind(now * 1000L, "key")).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    verify(statement).bind(eq(now * 1000L), eq("key"));
    assertTrue(boundStatement.isPresent());
    assertSame(bound, boundStatement.get());
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "DELETE my_attribute FROM my_table USING TIMESTAMP ? WHERE hgw=?",
        preparedStatement.get(0));
  }

  @Test
  public void testIngestDeleteWildcard() {
    long now = System.currentTimeMillis();
    StreamElement ingest =
        StreamElement.delete(
            entity, attrWildcard, UUID.randomUUID().toString(), "key", "device.1", now);
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind(now * 1000L, "1", "key")).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    verify(statement).bind(eq(now * 1000L), eq("1"), eq("key"));
    assertTrue(boundStatement.isPresent());
    assertSame(bound, boundStatement.get());
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "DELETE my_col FROM my_table USING TIMESTAMP ? WHERE device=? AND hgw=?",
        preparedStatement.get(0));
  }

  @Test
  public void testIngestDeleteWildcardAll() {
    long now = System.currentTimeMillis();
    StreamElement ingest =
        StreamElement.deleteWildcard(
            entity, attrWildcard, UUID.randomUUID().toString(), "key", now);
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind(now * 1000L, "key")).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    verify(statement).bind(eq(now * 1000L), eq("key"));
    assertTrue(boundStatement.isPresent());
    assertEquals(1, preparedStatement.size());
    assertEquals("DELETE FROM my_table USING TIMESTAMP ? WHERE hgw=?", preparedStatement.get(0));
  }

  @Test
  public void testGetAttribute() {
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key")).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    BoundStatement boundStatement = factory.getReadStatement("key", attr.getName(), attr, session);
    verify(statement).bind(eq("key"));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals("SELECT my_attribute FROM my_table WHERE hgw=?", preparedStatement.get(0));
  }

  @Test
  public void testGetAttributeWildcard() {
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", "1")).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    BoundStatement boundStatement =
        factory.getReadStatement("key", "device.1", attrWildcard, session);

    verify(statement).bind(eq("key"), eq("1"));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals("SELECT my_col FROM my_table WHERE hgw=? AND device=?", preparedStatement.get(0));
  }

  @Test
  public void testGetAttributeWildcardWithNonCharacterSuffix() {
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", "1:2")).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    BoundStatement boundStatement =
        factory.getReadStatement("key", "device.1:2", attrWildcard, session);

    verify(statement).bind(eq("key"), eq("1:2"));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals("SELECT my_col FROM my_table WHERE hgw=? AND device=?", preparedStatement.get(0));
  }

  @Test
  public void testListWildcardWithoutStart() {
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", "", Integer.MAX_VALUE)).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    BoundStatement boundStatement =
        factory.getListStatement("key", attrWildcard, null, -1, session);
    verify(statement).bind(eq("key"), eq(""), eq(Integer.MAX_VALUE));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "SELECT device, my_col FROM my_table WHERE hgw=? AND device>? LIMIT ?",
        preparedStatement.get(0));
  }

  @Test
  public void testListWildcardWithStart() {
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", "1", 10)).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    BoundStatement boundStatement =
        factory.getListStatement("key", attrWildcard, new Offsets.Raw("device.1"), 10, session);
    verify(statement).bind(eq("key"), eq("1"), eq(10));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "SELECT device, my_col FROM my_table WHERE hgw=? AND device>? LIMIT ?",
        preparedStatement.get(0));
  }

  @Test
  public void testFetchOffset() {
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key")).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    BoundStatement boundStatement = factory.getFetchTokenStatement("key", session);
    verify(statement).bind(eq("key"));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals("SELECT token(hgw) FROM my_table WHERE hgw=?", preparedStatement.get(0));
  }

  @Test
  public void testListWildcardWithExplicitSecondaryField() {
    factory.setup(
        entity,
        URI.create("cassandra://wherever/my_table?data=my_col" + "&primary=hgw&secondary=stamp"),
        StringConverter.getDefault());
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind("key", "", Integer.MAX_VALUE)).thenReturn(bound);
    when(session.prepare((String) any())).thenReturn(statement);

    BoundStatement boundStatement =
        factory.getListStatement("key", attrWildcard, null, -1, session);
    verify(statement).bind(eq("key"), eq(""), eq(Integer.MAX_VALUE));
    assertNotNull("Bound statement cannot be null", boundStatement);
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "SELECT stamp, my_col FROM my_table WHERE hgw=? AND stamp>? LIMIT ?",
        preparedStatement.get(0));
  }

  @Test
  public void testScanPartition() {
    Statement statement =
        factory.scanPartition(
            Collections.singletonList(attr),
            new CassandraPartition(0, Long.MIN_VALUE, Long.MAX_VALUE, 0, 100, false),
            null);
    assertTrue(statement instanceof SimpleStatement);
    assertEquals(
        "SELECT hgw, my_attribute FROM my_table WHERE token(hgw) >= 0 AND token(hgw) < 100",
        ((SimpleStatement) statement).getQuery());

    statement =
        factory.scanPartition(
            Collections.singletonList(attrWildcard),
            new CassandraPartition(0, Long.MIN_VALUE, Long.MAX_VALUE, 0, 100, true),
            null);
    assertTrue(statement instanceof SimpleStatement);
    assertEquals(
        "SELECT hgw, device, my_col FROM my_table WHERE token(hgw) >= 0 AND token(hgw) <= 100",
        ((SimpleStatement) statement).getQuery());
  }

  @Test
  public void testV2SerializerIngest() {
    factory.setup(
        entity,
        URI.create("cassandra://whatever/my_table?primary=hgw&serializer=v2"),
        StringConverter.getDefault());

    long now = System.currentTimeMillis();
    StreamElement ingest =
        StreamElement.upsert(entity, attr, 1001L, "key", "myAttribute", now, "value".getBytes());
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind(eq("key"), any(), eq(now * 1000L)))
        .thenAnswer(
            invocationOnMock -> {
              ByteBuffer bytes = invocationOnMock.getArgument(1);
              Cell cell = Cell.parseFrom(ByteString.copyFrom(bytes));
              assertEquals("value", cell.getValue().toStringUtf8());
              assertEquals(1001L, cell.getSeqId());
              KeyValue<?> received =
                  factory.toKeyValue(
                      entity,
                      attrWildcard,
                      "key",
                      "device.1",
                      now,
                      Offsets.empty(),
                      bytes.slice().array());
              assertEquals(ingest.getKey(), received.getKey());
              assertEquals(ingest.getSequentialId(), received.getSequentialId());
              return bound;
            });
    when(session.prepare((String) any())).thenReturn(statement);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    assertTrue(boundStatement.isPresent());
    assertSame(bound, boundStatement.get());
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "INSERT INTO my_table (hgw, my_attribute) VALUES (?, ?) USING TIMESTAMP ?",
        preparedStatement.get(0));
  }

  @Test
  public void testV2SerializerIngestWildcard() {

    factory.setup(
        entity,
        URI.create("cassandra://whatever/my_table?primary=hgw&data=my_col&serializer=v2"),
        StringConverter.getDefault());

    long now = System.currentTimeMillis();
    StreamElement ingest =
        StreamElement.upsert(
            entity, attrWildcard, 1001L, "key", "device.1", now, "value".getBytes());
    BoundStatement bound = mock(BoundStatement.class);
    when(statement.bind(eq("key"), eq("1"), any(), eq(now * 1000L)))
        .thenAnswer(
            invocationOnMock -> {
              ByteBuffer bytes = invocationOnMock.getArgument(2);
              Cell cell = Cell.parseFrom(ByteString.copyFrom(bytes));
              assertEquals(1001L, cell.getSeqId());
              assertEquals("value", cell.getValue().toStringUtf8());
              KeyValue<?> received =
                  factory.toKeyValue(
                      entity,
                      attrWildcard,
                      "key",
                      "device.1",
                      now,
                      Offsets.empty(),
                      bytes.slice().array());
              assertEquals(ingest.getKey(), received.getKey());
              assertEquals(ingest.getSequentialId(), received.getSequentialId());
              return bound;
            });
    when(session.prepare((String) any())).thenReturn(statement);

    Optional<BoundStatement> boundStatement = factory.getWriteStatement(ingest, session);
    assertTrue(boundStatement.isPresent());
    assertSame(boundStatement.get(), bound);
    assertEquals(1, preparedStatement.size());
    assertEquals(
        "INSERT INTO my_table (hgw, device, my_col) VALUES (?, ?, ?) USING TIMESTAMP ?",
        preparedStatement.get(0));
  }

  @Test
  public void testV2SerializerRead() {
    factory.setup(
        entity,
        URI.create("cassandra://whatever/my_table?primary=hgw&data=my_col&serializer=v2"),
        StringConverter.getDefault());
    long now = System.currentTimeMillis();
    assertNotNull(
        factory.toKeyValue(
            entity,
            attrWildcard,
            "key",
            attrWildcard.toAttributePrefix(true) + "1",
            now,
            Offsets.empty(),
            Cell.newBuilder()
                .setSeqId(1L)
                .setValue(ByteString.copyFrom(new byte[] {1}))
                .build()
                .toByteArray()));
    assertNull(
        factory.toKeyValue(
            entity,
            attrWildcard,
            "key",
            attrWildcard.toAttributePrefix(true) + "1",
            System.currentTimeMillis(),
            Offsets.empty(),
            new byte[] {(byte) 199, 0}));
  }
}
