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
package cz.o2.proxima.direct.io.hadoop;

import static org.junit.Assert.*;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.core.repository.ConfigRepository;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.internal.AbstractDataAccessorFactory.Accept;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.core.batch.ObserveHandle;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@Slf4j
public class HadoopStorageTest {

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final Repository repository =
      ConfigRepository.Builder.ofTest(ConfigFactory.load("test-reference.conf").resolve()).build();
  private final DirectDataOperator direct =
      repository.getOrCreateOperator(DirectDataOperator.class);
  ;
  private final EntityDescriptor entity = repository.getEntity("gateway");
  private final AttributeDescriptor<byte[]> attribute = entity.getAttribute("armed");

  File root;
  URI uri;

  @Before
  public void setUp() throws IOException {
    root = tempFolder.newFolder();
    uri = URI.create(String.format("hadoop:file://%s", root.getAbsolutePath()));
  }

  @Test
  public void testSerialize() throws IOException, ClassNotFoundException {
    HadoopStorage storage = new HadoopStorage();
    TestUtils.assertSerializable(storage);
  }

  @Test
  public void testHashCodeAndEquals() {
    TestUtils.assertHashCodeAndEquals(new HadoopStorage(), new HadoopStorage());
    EntityDescriptor entity = EntityDescriptor.newBuilder().setName("dummy").build();
    final AttributeFamilyDescriptor family =
        TestUtils.createTestFamily(entity, URI.create("hdfs://host:9000/path"));
    TestUtils.assertHashCodeAndEquals(
        new HadoopDataAccessor(family), new HadoopDataAccessor(family));
  }

  @Test
  public void testAcceptScheme() {
    HadoopStorage storage = new HadoopStorage();
    assertEquals(Accept.ACCEPT, storage.accepts(URI.create("hdfs://host:9000/path")));
    assertEquals(Accept.ACCEPT, storage.accepts(URI.create("hadoop:file:///path")));
    assertEquals(Accept.REJECT, storage.accepts(URI.create("file:///path")));
  }

  @Test
  public void testSchemeRemap() {
    URI remap = HadoopStorage.remap(URI.create("hdfs://authority/path"));
    assertEquals("hdfs", remap.getScheme());
    assertEquals("authority", remap.getAuthority());
    assertEquals("/path", remap.getPath());
    remap = HadoopStorage.remap(URI.create("hdfs://authority/"));
    assertEquals("hdfs", remap.getScheme());
    assertEquals("authority", remap.getAuthority());
    assertEquals("/", remap.getPath());
    remap = HadoopStorage.remap(URI.create("hadoop:file:///"));
    assertEquals("file", remap.getScheme());
    assertNull(remap.getAuthority());
    assertEquals("/", remap.getPath());
    remap = HadoopStorage.remap(URI.create("hadoop:file:///tmp/?query=a"));
    assertEquals("file", remap.getScheme());
    assertNull(remap.getAuthority());
    assertEquals("/tmp/", remap.getPath());
    assertEquals("query=a", remap.getQuery());
    final URI invalidURI = URI.create("hadoop:///tmp/");
    assertThrows(IllegalArgumentException.class, () -> HadoopStorage.remap(invalidURI));
  }

  @Test(timeout = 5000L)
  public void testWriteElement() throws InterruptedException {
    Map<String, Object> cfg = cfg(HadoopDataAccessor.HADOOP_ROLL_INTERVAL, -1);
    HadoopDataAccessor accessor =
        new HadoopDataAccessor(TestUtils.createTestFamily(entity, uri, cfg));

    CountDownLatch latch = new CountDownLatch(1);
    BulkAttributeWriter writer =
        writeOneElement(
            accessor,
            (success, error) -> {
              assertTrue(success);
              assertNull(error);
              latch.countDown();
            });
    writer.updateWatermark(Long.MAX_VALUE);
    latch.await();
    assertTrue(root.exists());
    List<File> files = listRecursively(root);
    assertEquals("Expected single file in " + files, 1, files.size());
    assertFalse(Iterables.getOnlyElement(files).getAbsolutePath().contains("_tmp"));

    BatchLogReader reader = accessor.getBatchLogReader(direct.getContext()).orElse(null);
    assertNotNull(reader);
    List<Partition> partitions = reader.getPartitions();
    assertEquals(1, partitions.size());
    BlockingQueue<StreamElement> queue = new SynchronousQueue<>();
    reader.observe(
        partitions,
        Collections.singletonList(attribute),
        new BatchLogObserver() {
          @Override
          public boolean onNext(StreamElement element) {
            ExceptionUtils.unchecked(() -> queue.put(element));
            return true;
          }
        });
    StreamElement element = queue.take();
    assertNotNull(element);
  }

  @Test(timeout = 5000L)
  public void testObserveCancel() throws InterruptedException {
    Map<String, Object> cfg = cfg(HadoopDataAccessor.HADOOP_ROLL_INTERVAL, -1);
    HadoopDataAccessor accessor =
        new HadoopDataAccessor(TestUtils.createTestFamily(entity, uri, cfg));

    CountDownLatch latch = new CountDownLatch(1);
    writeOneElement(
            accessor,
            (success, error) -> {
              assertTrue(success);
              assertNull(error);
              latch.countDown();
            })
        .updateWatermark(Long.MAX_VALUE);
    latch.await();
    BatchLogReader reader = accessor.getBatchLogReader(direct.getContext()).orElse(null);
    assertNotNull(reader);
    List<Partition> partitions = reader.getPartitions();
    assertEquals(1, partitions.size());
    CountDownLatch cancelledLatch = new CountDownLatch(1);
    ObserveHandle handle =
        reader.observe(
            partitions,
            Collections.singletonList(attribute),
            new BatchLogObserver() {
              @Override
              public boolean onNext(StreamElement element) {
                ExceptionUtils.unchecked(() -> TimeUnit.MILLISECONDS.sleep(100));
                return true;
              }

              @Override
              public void onCompleted() {
                fail("onCompleted should not have been called");
              }

              @Override
              public void onCancelled() {
                cancelledLatch.countDown();
              }

              @Override
              public boolean onError(Throwable error) {
                onCancelled();
                return true;
              }
            });
    handle.close();
    cancelledLatch.await();
  }

  @Test(timeout = 5000L)
  public void testOnNextCancel() throws InterruptedException {
    Map<String, Object> cfg = cfg(HadoopDataAccessor.HADOOP_ROLL_INTERVAL, -1);
    HadoopDataAccessor accessor =
        new HadoopDataAccessor(TestUtils.createTestFamily(entity, uri, cfg));

    long now = System.currentTimeMillis();
    AtomicInteger consumed = new AtomicInteger();
    CountDownLatch latch = new CountDownLatch(1);
    write(
            accessor,
            (success, error) -> {
              assertTrue(success);
              assertNull(error);
              latch.countDown();
            },
            element(now),
            element(now + 1))
        .updateWatermark(Long.MAX_VALUE);
    latch.await();
    BatchLogReader reader = accessor.getBatchLogReader(direct.getContext()).orElse(null);
    assertNotNull(reader);
    List<Partition> partitions = reader.getPartitions();
    assertEquals(1, partitions.size());
    CountDownLatch consumedLatch = new CountDownLatch(1);
    reader.observe(
        partitions,
        Collections.singletonList(attribute),
        new BatchLogObserver() {
          @Override
          public boolean onNext(StreamElement element) {
            consumed.incrementAndGet();
            return true;
          }

          @Override
          public void onCompleted() {
            consumedLatch.countDown();
          }

          @Override
          public void onCancelled() {
            fail("onCompleted should not have been called");
          }
        });
    consumedLatch.await();
    assertEquals(1, consumed.get());
  }

  @Test(timeout = 5000L)
  public void testWriteElementJson() throws InterruptedException {
    Map<String, Object> cfg =
        cfg(HadoopDataAccessor.HADOOP_ROLL_INTERVAL, -1, "hadoop.format", "json");
    HadoopDataAccessor accessor =
        new HadoopDataAccessor(TestUtils.createTestFamily(entity, uri, cfg));

    CountDownLatch latch = new CountDownLatch(1);
    BulkAttributeWriter writer =
        writeOneElement(
            accessor,
            ((success, error) -> {
              assertTrue(success);
              assertNull(error);
              latch.countDown();
            }));

    writer.updateWatermark(Long.MAX_VALUE);
    latch.await();
    assertTrue(root.exists());
    List<File> files = listRecursively(root);
    assertEquals("Expected single file in " + files, 1, files.size());
    assertFalse(Iterables.getOnlyElement(files).getAbsolutePath().contains("_tmp"));

    BatchLogReader reader = accessor.getBatchLogReader(direct.getContext()).orElse(null);
    assertNotNull(reader);
    List<Partition> partitions = reader.getPartitions();
    assertEquals(1, partitions.size());
    BlockingQueue<StreamElement> queue = new SynchronousQueue<>();
    reader.observe(
        partitions,
        Collections.singletonList(attribute),
        new BatchLogObserver() {
          @Override
          public boolean onNext(StreamElement element) {
            ExceptionUtils.unchecked(() -> queue.put(element));
            return true;
          }
        });
    StreamElement element = queue.take();
    assertNotNull(element);
  }

  @Test(timeout = 5_000L)
  public void testWriteElementNotYetFlushed() throws InterruptedException {
    Map<String, Object> cfg = cfg(HadoopDataAccessor.HADOOP_ROLL_INTERVAL, 1000);
    HadoopDataAccessor accessor =
        new HadoopDataAccessor(TestUtils.createTestFamily(entity, uri, cfg));

    CountDownLatch latch = new CountDownLatch(1);
    BulkAttributeWriter writer =
        writeOneElement(
            accessor,
            ((success, error) -> {
              if (error != null) {
                log.error("Failed to flush write", error);
              }
              assertTrue("Error in flush " + error, success);
              assertNull(error);
              latch.countDown();
            }));
    assertTrue(root.exists());
    List<File> files = listRecursively(root);
    assertEquals("Expected single file in " + files, 1, files.size());
    assertTrue(Iterables.getOnlyElement(files).getAbsolutePath().contains("_tmp"));

    BatchLogReader reader = accessor.getBatchLogReader(direct.getContext()).orElse(null);
    assertNotNull(reader);
    List<Partition> partitions = reader.getPartitions();
    assertTrue("Expected empty partitions, got " + partitions, partitions.isEmpty());

    // advance watermark to flush
    writer.updateWatermark(Long.MAX_VALUE);

    latch.await();

    partitions = reader.getPartitions();
    assertEquals(1, partitions.size());
    BlockingQueue<StreamElement> queue = new SynchronousQueue<>();
    reader.observe(
        partitions,
        Collections.singletonList(attribute),
        new BatchLogObserver() {
          @Override
          public boolean onNext(StreamElement element) {
            ExceptionUtils.unchecked(() -> queue.put(element));
            return true;
          }
        });
    StreamElement element = queue.take();
    assertNotNull(element);
  }

  @Test
  public void testWriterAsFactorySerializable() throws IOException, ClassNotFoundException {
    HadoopDataAccessor accessor =
        new HadoopDataAccessor(TestUtils.createTestFamily(entity, URI.create("hdfs://namenode")));
    HadoopBulkAttributeWriter writer = new HadoopBulkAttributeWriter(accessor, direct.getContext());
    byte[] bytes = TestUtils.serializeObject(writer.asFactory());
    AttributeWriterBase.Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(writer.getUri(), factory.apply(repository).getUri());
  }

  @Test
  public void testReaderAsFactorySerializable() throws IOException, ClassNotFoundException {
    HadoopDataAccessor accessor =
        new HadoopDataAccessor(TestUtils.createTestFamily(entity, URI.create("hdfs://namenode")));
    HadoopBatchLogReader reader = new HadoopBatchLogReader(accessor, direct.getContext());
    byte[] bytes = TestUtils.serializeObject(reader.asFactory());
    BatchLogReader.Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(
        accessor.getUri(),
        ((HadoopBatchLogReader) factory.apply(repository)).getAccessor().getUri());
  }

  Map<String, Object> cfg(Object... kvs) {
    Preconditions.checkArgument(kvs.length % 2 == 0);
    Map<String, Object> ret = new HashMap<>();
    String key = null;
    for (Object kv : kvs) {
      if (key == null) {
        key = kv.toString();
      } else {
        ret.put(key, kv);
        key = null;
      }
    }
    return ret;
  }

  private BulkAttributeWriter writeOneElement(
      HadoopDataAccessor accessor, CommitCallback callback) {
    StreamElement element = element(System.currentTimeMillis());
    return write(accessor, callback, element);
  }

  private BulkAttributeWriter write(
      HadoopDataAccessor accessor, CommitCallback callback, StreamElement... elements) {

    Optional<AttributeWriterBase> writer = accessor.newWriter(direct.getContext());
    assertTrue(writer.isPresent());

    BulkAttributeWriter bulk = writer.get().bulk();

    for (StreamElement el : elements) {
      bulk.write(el, el.getStamp(), callback);
    }
    return bulk;
  }

  private StreamElement element(long stamp) {
    return StreamElement.upsert(
        entity,
        attribute,
        UUID.randomUUID().toString(),
        "test",
        attribute.getName(),
        stamp,
        "test value".getBytes());
  }

  private List<File> listRecursively(File dir) {
    if (dir.isFile()) {
      if (!dir.getName().endsWith(".crc")) {
        return Collections.singletonList(dir);
      } else {
        return Collections.emptyList();
      }
    }
    List<File> ret = new ArrayList<>();
    for (File f : Objects.requireNonNull(dir.listFiles())) {
      ret.addAll(listRecursively(f));
    }
    return ret;
  }
}
