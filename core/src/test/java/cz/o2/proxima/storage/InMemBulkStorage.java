/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage;

import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import cz.o2.proxima.storage.randomaccess.RandomOffset;
import cz.o2.proxima.storage.randomaccess.RawOffset;
import cz.o2.proxima.util.Pair;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Storage acting as a bulk in memory storage.
 */
@Slf4j
public class InMemBulkStorage extends StorageDescriptor {

  private static InMemBulkStorage THIS;

  private class Writer extends AbstractBulkAttributeWriter {

    int writtenSinceLastCommit = 0;
    @Nullable CommitCallback lastCallback = null;

    public Writer(EntityDescriptor entityDesc, URI uri) {
      super(entityDesc, uri);
    }

    @Override
    public void write(StreamElement ingest, CommitCallback statusCallback) {
      log.debug("Writing {} into {}", ingest, getURI());
      // store the data, commit after each 10 elements
      data.put(
          getURI().getPath() + "/" + ingest.getKey() + "#" + ingest.getAttribute(),
          ingest.getValue());
      lastCallback = statusCallback;
      if (++writtenSinceLastCommit >= 10) {
        flush();
      }
    }

    @Override
    public void rollback() {
      // nop
    }

    @Override
    public void flush() {
      if (lastCallback != null) {
        lastCallback.commit(true, null);
        lastCallback = null;
      }
      writtenSinceLastCommit = 0;
    }

  }

  private class Reader extends AbstractStorage implements RandomAccessReader {

    public Reader(EntityDescriptor entityDesc, URI uri) {
      super(entityDesc, uri);
    }

    @Override
    public RandomOffset fetchOffset(Listing type, String key) {
      return new RawOffset(key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Optional<KeyValue<T>> get(
        String key, String attribute, AttributeDescriptor<T> desc) {

      return Optional
          .ofNullable(data.get(getURI().getPath() + "/" + key + "#" + attribute))
          .map(b -> (KeyValue) KeyValue.of(
              getEntityDescriptor(),
              getEntityDescriptor().findAttribute(attribute).get(),
              key,
              attribute,
              new RawOffset(attribute),
              b));
    }

    @Override
    public void scanWildcardAll(
        String key, RandomOffset offset, int limit, Consumer<KeyValue<?>> consumer) {

      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <T> void scanWildcard(
        String key, AttributeDescriptor<T> wildcard, RandomOffset offset,
        int limit, Consumer<KeyValue<T>> consumer) {

      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void listEntities(
        RandomOffset offset, int limit,
        Consumer<Pair<RandomOffset, String>> consumer) {

      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close() throws IOException {
      // nop
    }

  }

  private class InMemBulkAccessor implements DataAccessor {

    private final EntityDescriptor entityDesc;
    private final URI uri;

    InMemBulkAccessor(EntityDescriptor entityDesc, URI uri) {
      this.entityDesc = entityDesc;
      this.uri = uri;
    }

    @Override
    public Optional<AttributeWriterBase> getWriter(Context context) {
      return Optional.of(new Writer(entityDesc, uri));
    }

    @Override
    public Optional<RandomAccessReader> getRandomAccessReader(Context context) {
      return Optional.of(new Reader(entityDesc, uri));
    }

  }

  @Getter
  private final NavigableMap<String, byte[]> data = new TreeMap<>();

  public InMemBulkStorage() {
    super(Collections.singletonList("inmem-bulk"));
    THIS = this;
  }

  @Override
  public DataAccessor getAccessor(
      EntityDescriptor entityDesc, URI uri,
      Map<String, Object> cfg) {

    return new InMemBulkAccessor(entityDesc, uri);
  }

  // on deserialization use the singleton instance
  private Object readResolve() {
    return Objects.requireNonNull(THIS);
  }

}
