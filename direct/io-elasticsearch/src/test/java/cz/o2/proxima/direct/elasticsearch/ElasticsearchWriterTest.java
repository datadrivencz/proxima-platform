/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.elasticsearch;

import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.elasticsearch.ElasticsearchWriter.BulkProcessorListener;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Optionals;
import cz.o2.proxima.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class ElasticsearchWriterTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load("test-es.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final Regular<Float> metric = Regular.of(gateway, gateway.getAttribute("metric"));

  @Test
  public void testWriterToJson() {
    ElasticsearchStorage storage = new ElasticsearchStorage();
    ElasticsearchAccessor accessor =
        storage.createAccessor(direct, repo.getFamilyByName("gateway-to-es"));
    ElasticsearchWriter writer =
        (ElasticsearchWriter) Optionals.get(accessor.getWriter(direct.getContext()));
    StreamElement element = metric.upsert("key", System.currentTimeMillis(), 1.0f);
    String json = writer.toJson(element);
    JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
    assertEquals("key", obj.get("key").getAsString());
    assertEquals("gateway", obj.get("entity").getAsString());
    assertEquals("metric", obj.get("attribute").getAsString());
    assertTrue(obj.has("timestamp"));
    assertTrue(obj.has("updated_at"));
    assertTrue(obj.has("uuid"));
    assertEquals(1.0f, obj.get("data").getAsFloat(), 0.0001);
    assertEquals("key:metric", ElasticsearchWriter.toEsKey(element));
  }

  @Test
  @Timeout(5)
  public void testBulkWrites() throws InterruptedException {
    ElasticsearchStorage storage = new ElasticsearchStorage();
    ElasticsearchAccessor accessor =
        storage.createAccessor(direct, repo.getFamilyByName("gateway-to-es"));
    List<IndexRequest> indexRequestList = new ArrayList<>();
    ElasticsearchWriter writer =
        new ElasticsearchWriter(accessor) {
          @Override
          BulkWriter createBulkWriter(ElasticsearchAccessor accessor) {
            return new BulkWriter() {
              @Override
              public void add(IndexRequest request, CommitCallback commit) {
                indexRequestList.add(request);
                commit.commit(true, null);
              }

              @Override
              public void add(DeleteRequest request, CommitCallback commit) {}

              @Override
              public void close() {}
            };
          }
        };
    long now = System.currentTimeMillis();
    StreamElement element = metric.upsert("key", now, 1.0f);
    BlockingQueue<Pair<Boolean, Throwable>> res = new ArrayBlockingQueue<>(1);
    writer.write(
        element, now, (succ, exc) -> ExceptionUtils.unchecked(() -> res.put(Pair.of(succ, exc))));
    Pair<Boolean, Throwable> taken = res.take();
    assertTrue(taken.getFirst());
    assertNull(taken.getSecond());
    assertEquals(1, indexRequestList.size());
    JsonObject json =
        JsonParser.parseString(indexRequestList.get(0).source().utf8ToString()).getAsJsonObject();
    assertEquals(1.0f, json.get("data").getAsFloat(), 0.001);
    assertEquals("my_index", indexRequestList.get(0).index());
    assertEquals("key:metric", indexRequestList.get(0).id());
  }

  @Test
  public void testBulkProcessorListener() {
    NavigableMap<Long, CommitCallback> pendingCommits = new TreeMap<>();
    NavigableMap<Long, CommitCallback> confirmedCommits = new TreeMap<>();
    BulkProcessorListener listener = new BulkProcessorListener(pendingCommits, confirmedCommits);
    AtomicReference<Pair<Boolean, Integer>> commit = new AtomicReference<>();
    listener.setLastWrittenOffset(committed(commit, 1));
    listener.beforeBulk(1, null);
    listener.afterBulk(1, null, (BulkResponse) null);
    assertEquals(1, commit.get().getSecond());
    assertTrue(commit.get().getFirst());

    listener.setLastWrittenOffset(committed(commit, 2));
    listener.beforeBulk(2, null);
    listener.setLastWrittenOffset(committed(commit, 3));
    listener.beforeBulk(3, null);
    listener.afterBulk(3, null, (BulkResponse) null);
    assertEquals(1, commit.get().getSecond());
    listener.afterBulk(2, null, (BulkResponse) null);
    assertEquals(3, commit.get().getSecond());

    listener.setLastWrittenOffset(committed(commit, 4));
    listener.beforeBulk(4, null);
    listener.afterBulk(4, null, new RuntimeException("ex"));
    assertEquals(4, commit.get().getSecond());
    assertFalse(commit.get().getFirst());
  }

  private CommitCallback committed(AtomicReference<Pair<Boolean, Integer>> commit, int value) {
    return (succ, exc) -> commit.set(Pair.of(succ, value));
  }
}
