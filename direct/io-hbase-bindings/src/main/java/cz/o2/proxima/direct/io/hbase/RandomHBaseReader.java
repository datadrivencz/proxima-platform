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
package cz.o2.proxima.direct.io.hbase;

import cz.o2.proxima.core.functional.Consumer;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader.Listing;
import cz.o2.proxima.direct.core.randomaccess.RandomOffset;
import cz.o2.proxima.direct.core.randomaccess.RawOffset;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

/** {@code RandomAccessReader} for HBase. */
@Slf4j
@EqualsAndHashCode
public class RandomHBaseReader extends HBaseClientWrapper implements RandomAccessReader {

  private static final String KEYS_SCANNER_CACHING = "hbase.list-keys.caching";
  private static final int KEYS_SCANNER_CACHING_DEFAULT = 1000;

  private final EntityDescriptor entity;
  private final InternalSerializer serializer;
  private final int keyCaching;
  private final Map<String, Object> cfg;

  public RandomHBaseReader(
      URI uri, Configuration conf, Map<String, Object> cfg, EntityDescriptor entity) {

    super(uri, conf);
    this.entity = entity;
    this.serializer = HBaseDataAccessor.instantiateSerializer(uri);
    this.keyCaching =
        Integer.parseInt(
            Optional.ofNullable(cfg.get(KEYS_SCANNER_CACHING))
                .orElse(KEYS_SCANNER_CACHING_DEFAULT)
                .toString());
    this.cfg = cfg;
  }

  @Override
  public RandomOffset fetchOffset(Listing type, String key) {
    return new RawOffset(key);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Optional<KeyValue<T>> get(
      String key, String attribute, AttributeDescriptor<T> desc, long stamp) {

    ensureClient();
    byte[] qualifier = attribute.getBytes(StandardCharsets.UTF_8);
    Get get = new Get(key.getBytes(StandardCharsets.UTF_8));
    get.addColumn(family, qualifier);
    try {
      Result res = client.get(get);
      Cell cell = res.getColumnLatestCell(family, qualifier);
      return Optional.ofNullable(cell == null ? null : serializer.toKeyValue(entity, desc, cell));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void scanWildcard(
      String key,
      AttributeDescriptor<T> wildcard,
      RandomOffset offset,
      long stamp,
      int limit,
      Consumer<KeyValue<T>> consumer) {

    try {
      ensureClient();
      final RawOffset stroff = (RawOffset) offset;
      final Get get = new Get(key.getBytes(StandardCharsets.UTF_8));
      get.addFamily(family);
      Filter filter =
          new ColumnPrefixFilter((wildcard.toAttributePrefix()).getBytes(StandardCharsets.UTF_8));
      final Scan scan = new Scan(get);
      if (limit <= 0) {
        limit = Integer.MAX_VALUE;
      }
      scan.setBatch(limit);
      if (stroff != null) {
        filter =
            new FilterList(
                filter,
                new ColumnPaginationFilter(
                    limit, (stroff.getOffset() + '\00').getBytes(StandardCharsets.UTF_8)));
      }
      scan.setFilter(filter);
      int accepted = 0;
      try (ResultScanner scanner = client.getScanner(scan)) {
        Result next;
        while (accepted < limit && (next = scanner.next()) != null) {
          CellScanner cellScanner = next.cellScanner();
          while (cellScanner.advance() && accepted++ < limit) {
            Cell cell = cellScanner.current();
            consumer.accept(serializer.toKeyValue(entity, wildcard, cell));
          }
        }
      }

    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void listEntities(
      RandomOffset offset, int limit, Consumer<Pair<RandomOffset, String>> consumer) {

    ensureClient();
    Scan s =
        offset == null
            ? new Scan()
            : new Scan((((RawOffset) offset).getOffset() + '\00').getBytes(StandardCharsets.UTF_8));
    s.addFamily(family);
    s.setFilter(new KeyOnlyFilter());

    s.setCaching(keyCaching);
    try (ResultScanner scanner = client.getScanner(s)) {
      int taken = 0;
      while (limit <= 0 || taken++ < limit) {
        Result res = scanner.next();
        if (res != null) {
          String key = new String(res.getRow());
          consumer.accept(Pair.of(new RawOffset(key), key));
        } else {
          break;
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void close() {
    if (client != null) {
      Util.closeQuietly(client);
      client = null;
    }
  }

  @Override
  public EntityDescriptor getEntityDescriptor() {
    return entity;
  }

  @Override
  public Factory<?> asFactory() {
    final URI uri = getUri();
    byte[] serializedConf = this.serializedConf;
    final Map<String, Object> cfg = this.cfg;
    final EntityDescriptor entity = getEntityDescriptor();
    return repo ->
        new RandomHBaseReader(uri, deserialize(serializedConf, new Configuration()), cfg, entity);
  }

  @Override
  public void scanWildcardAll(
      String key, RandomOffset offset, long stamp, int limit, Consumer<KeyValue<?>> consumer) {

    throw new UnsupportedOperationException(
        "Unsupported. See https://github.com/O2-Czech-Republic/proxima-platform/issues/68");
  }
}
