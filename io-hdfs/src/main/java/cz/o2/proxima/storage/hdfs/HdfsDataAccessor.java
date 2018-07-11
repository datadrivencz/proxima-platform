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
package cz.o2.proxima.storage.hdfs;

import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.DataAccessor;
import cz.o2.proxima.storage.batch.BatchLogObservable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * {@code DataAccessor} for Hadoop Distributed FileSystem.
 */
@Slf4j
public class HdfsDataAccessor implements DataAccessor {

  public static final String HDFS_MIN_ELEMENTS_TO_FLUSH = "hdfs.min-elements-to-flush";
  public static final String HDFS_ROLL_INTERVAL = "hdfs.log-roll-interval";
  public static final String HDFS_BATCH_PROCESS_SIZE_MIN = "hdfs.process-size.min";

  static final int HDFS_MIN_ELEMENTS_TO_FLUSH_DEFAULT = 500;
  static final long HDFS_ROLL_INTERVAL_DEFAULT = TimeUnit.HOURS.toMillis(1);
  static final long HDFS_BATCH_PROCES_SIZE_MIN_DEFAULT = 1024 * 1024 * 100; /* 100 MiB */

  static final Pattern PART_FILE_PARSER = Pattern.compile("part-([0-9]+)_([0-9]+)-.+");
  static final DateTimeFormatter DIR_FORMAT = DateTimeFormatter.ofPattern("/yyyy/MM/");


  private final EntityDescriptor entityDesc;
  private final URI uri;
  private final Map<String, Object> cfg;

  private final int minElementsToFlush;
  private final long rollInterval;
  private final long batchProcessSize;


  public HdfsDataAccessor(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {
    this.entityDesc = entityDesc;
    this.uri = uri;
    this.cfg = cfg;
    this.minElementsToFlush = getCfg(
        HDFS_MIN_ELEMENTS_TO_FLUSH, cfg,
        o -> Integer.valueOf(o.toString()),
        HDFS_MIN_ELEMENTS_TO_FLUSH_DEFAULT);
    this.rollInterval = getCfg(
        HDFS_ROLL_INTERVAL, cfg,
        o -> Long.valueOf(o.toString()),
        HDFS_ROLL_INTERVAL_DEFAULT);
    this.batchProcessSize = getCfg(
        HDFS_BATCH_PROCESS_SIZE_MIN, cfg,
        o -> Long.valueOf(o.toString()),
        HDFS_BATCH_PROCES_SIZE_MIN_DEFAULT);
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.of(new HdfsBulkAttributeWriter(entityDesc, uri, cfg, minElementsToFlush, rollInterval));
  }

  @Override
  public Optional<BatchLogObservable> getBatchLogObservable(Context context) {
    return Optional.of(new HdfsBatchLogObservable(entityDesc, uri, cfg, context, batchProcessSize));
  }

  private <T> T getCfg(String name, Map<String, Object> cfg, Function<Object, T> convert, T defVal) {
    return Optional.ofNullable(cfg.get(name)).map(convert).orElse(defVal);
  }

  static FileSystem getFs(URI uri, Map<String, Object> cfg) {
    try {
      return FileSystem.get(uri, toHadoopConf(cfg));
    } catch (IOException ex) {
      throw new RuntimeException("Failed to get filesystem for URI: " + uri, ex);
    }
  }

  static Configuration toHadoopConf(Map<String, Object> cfg) {
    Configuration conf = new Configuration();
    cfg.forEach((key, value) -> conf.set(key, value.toString()));
    return conf;
  }
}
