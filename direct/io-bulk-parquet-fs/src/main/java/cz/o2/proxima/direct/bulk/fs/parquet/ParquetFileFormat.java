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
package cz.o2.proxima.direct.bulk.fs.parquet;

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.bulk.FileFormat;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.bulk.Reader;
import cz.o2.proxima.direct.bulk.Writer;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

@Internal
public class ParquetFileFormat implements FileFormat {

  public static final String PARQUET_CONFIG_COMPRESSION_KEY_NAME = ParquetOutputFormat.COMPRESSION;
  public static final String PARQUET_CONFIG_BLOCK_SIZE_KEY_NAME = ParquetOutputFormat.BLOCK_SIZE;
  public static final String PARQUET_CONFIG_ATTRIBUTES_PREFIX_KEY_NAME =
      "parquet.attribute.names.prefix";

  public static final int PARQUET_DEFAULT_BLOCK_SIZE = 1024 * 1024;
  public static final int PARQUET_DEFAULT_MAX_PADDING_BYTES = 512 * 1024;
  public static final String PARQUET_DEFAULT_ATTRIBUTE_NAMES_PREFIX = "attr_";
  static final String PARQUET_COLUMN_NAME_KEY = "key";
  static final String PARQUET_COLUMN_NAME_UUID = "uuid";
  static final String PARQUET_COLUMN_NAME_ATTRIBUTE = "attribute";
  static final String PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX = "attribute_prefix";
  static final String PARQUET_COLUMN_NAME_TIMESTAMP = "timestamp";
  static final String PARQUET_COLUMN_NAME_OPERATION = "operation";
  CompressionCodecName parquetCompressionCodec;
  private transient MessageType parquetSchema;
  private AttributeFamilyDescriptor familyDescriptor;
  private String attributeNamesPrefix;

  @Override
  public void setup(AttributeFamilyDescriptor family) {
    familyDescriptor = family;
    parquetCompressionCodec =
        CompressionCodecName.fromConf(
            Optional.ofNullable(family.getCfg().get(PARQUET_CONFIG_COMPRESSION_KEY_NAME))
                .map(Object::toString)
                .orElse(
                    Optional.ofNullable(family.getCfg().get("gzip"))
                        .filter(bool -> Boolean.parseBoolean(bool.toString()))
                        .map(bool -> CompressionCodecName.GZIP.name())
                        .orElse(null)));

    attributeNamesPrefix =
        Optional.ofNullable(family.getCfg().get(PARQUET_CONFIG_ATTRIBUTES_PREFIX_KEY_NAME))
            .map(Object::toString)
            .orElse(PARQUET_DEFAULT_ATTRIBUTE_NAMES_PREFIX);
  }

  @Override
  public Reader openReader(Path path, EntityDescriptor entity) throws IOException {
    return new ProximaParquetReader(path, entity, attributeNamesPrefix);
  }

  @Override
  public Writer openWriter(Path path, EntityDescriptor entity) throws IOException {
    return new ProximaParquetWriter(
        path, getParquetSchema(), attributeNamesPrefix, createWriterConfiguration());
  }

  Configuration createWriterConfiguration() {
    Configuration conf = new Configuration();
    Map<String, Object> familyConf = new HashMap<>(familyDescriptor.getCfg());
    familyConf.putIfAbsent(PARQUET_CONFIG_BLOCK_SIZE_KEY_NAME, PARQUET_DEFAULT_BLOCK_SIZE);
    familyConf.putIfAbsent(ParquetOutputFormat.PAGE_SIZE, PARQUET_DEFAULT_MAX_PADDING_BYTES);
    familyConf.putIfAbsent(PARQUET_CONFIG_COMPRESSION_KEY_NAME, parquetCompressionCodec.name());

    familyConf.forEach(
        (k, v) -> {
          if (k.startsWith("parquet.")) {
            conf.set(k, v.toString());
          }
        });
    return conf;
  }

  private MessageType getParquetSchema() {
    if (parquetSchema == null) {
      parquetSchema = ParquetUtils.createParquetSchema(familyDescriptor, attributeNamesPrefix);
    }
    return parquetSchema;
  }

  @Override
  public String fileSuffix() {
    return "parquet" + parquetCompressionCodec.getExtension();
  }

  enum OPERATION {
    UPSERT("u"),
    DELETE("d"),
    DELETE_WILDCARD("w");
    @Getter private final String value;

    OPERATION(String operation) {
      this.value = operation;
    }

    public static OPERATION of(String operation) {
      Preconditions.checkNotNull(operation);
      for (OPERATION op : values()) {
        if (op.getValue().equalsIgnoreCase(operation)) {
          return op;
        }
      }
      throw new IllegalArgumentException("Unknown operation " + operation);
    }
  }
}
