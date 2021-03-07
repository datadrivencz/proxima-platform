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
import com.google.common.collect.AbstractIterator;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.bulk.Reader;
import cz.o2.proxima.direct.bulk.fs.parquet.ParquetFileFormat.OPERATION;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.scheme.AttributeValueAccessors.ArrayValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValueAccessor;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.TypeConverter;

@Slf4j
public class ProximaParquetReader implements Reader {

  private final Path path;
  private final ParquetReader<StreamElement> reader;

  public ProximaParquetReader(Path path, EntityDescriptor entity) throws IOException {
    final SeekableByteChannel channel = (SeekableByteChannel) path.read();
    final Configuration configuration = new Configuration(false);
    this.reader =
        new ParquetReadBuilder(new BulkInputFile(channel), entity)
            .withConf(configuration)
            /**
             * Currently we can not use push down filter for attributes See
             * https://github.com/O2-Czech-Republic/proxima-platform/issues/196 for details
             * .withFilter()
             */
            .build();
    this.path = path;
  }

  @Override
  public void close() {
    ExceptionUtils.unchecked(reader::close);
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public Iterator<StreamElement> iterator() {
    return new AbstractIterator<StreamElement>() {
      @Override
      protected StreamElement computeNext() {
        try {
          StreamElement element = reader.read();
          if (element == null) {
            return endOfData();
          } else {
            return element;
          }
        } catch (IOException e) {
          throw new IllegalStateException("Unable to compute next element.", e);
        }
      }
    };
  }

  private static class ParquetReadBuilder extends ParquetReader.Builder<StreamElement> {

    private final EntityDescriptor entity;

    ParquetReadBuilder(InputFile file, EntityDescriptor entity) {
      super(file);
      this.entity = entity;
    }

    @Override
    protected ReadSupport<StreamElement> getReadSupport() {
      Preconditions.checkNotNull(entity, "Entity must be specified.");
      return new StreamElementReadSupport(entity);
    }
  }

  private static class StreamElementReadSupport extends ReadSupport<StreamElement> {

    private final EntityDescriptor entity;

    public StreamElementReadSupport(EntityDescriptor entity) {
      this.entity = entity;
    }

    @Override
    public ReadContext init(InitContext context) {
      return new ReadContext(context.getFileSchema());
    }

    @Override
    public RecordMaterializer<StreamElement> prepareForRead(
        Configuration configuration,
        Map<String, String> keyValueMetaData,
        MessageType fileSchema,
        ReadContext readContext) {
      final String attributeNamesPrefix =
          keyValueMetaData.getOrDefault(
              ParquetFileFormat.PARQUET_CONFIG_VALUES_PREFIX_KEY_NAME, "");
      return new InternalProximaRecordMaterializer(fileSchema, entity, attributeNamesPrefix);
      //return new StreamElementRecordMaterializer(fileSchema, entity, attributeNamesPrefix);
    }
  }

  private static class StreamElementRecordMaterializer extends RecordMaterializer<StreamElement> {

    private final GroupConverter root;
    private final EntityDescriptor entity;
    private final String attributeNamesPrefix;
    private final Map<String, Object> record = new HashMap<>();

    public StreamElementRecordMaterializer(
        MessageType schema, EntityDescriptor entity, String attributeNamesPrefix) {
      this.entity = entity;
      this.attributeNamesPrefix = attributeNamesPrefix;
      this.root =
          (GroupConverter)
              schema.convertWith(
                  new TypeConverter<Converter>() {
                    @Override
                    public Converter convertPrimitiveType(
                        List<GroupType> path, PrimitiveType primitiveType) {

                      return new PrimitiveConverter() {
                        @Override
                        public void addBinary(Binary value) {
                          if (primitiveType.getLogicalTypeAnnotation() != null) {
                            if (primitiveType
                                .getLogicalTypeAnnotation()
                                .equals(LogicalTypeAnnotation.stringType())) {
                              getMap(path).put(primitiveType.getName(), value.toStringUsingUTF8());
                            } else if (primitiveType
                                .getLogicalTypeAnnotation()
                                .equals(LogicalTypeAnnotation.enumType())) {
                              getMap(path)
                                  .put(primitiveType.getName(), new String(value.getBytes()));
                            } else {
                              getMap(path).put(primitiveType.getName(), value.getBytes());
                            }
                          } else {
                            getMap(path).put(primitiveType.getName(), value.getBytes());
                          }
                        }

                        @Override
                        public void addBoolean(boolean value) {
                          getMap(path).put(primitiveType.getName(), value);
                        }

                        @Override
                        public void addDouble(double value) {
                          getMap(path).put(primitiveType.getName(), value);
                        }

                        @Override
                        public void addFloat(float value) {
                          getMap(path).put(primitiveType.getName(), value);
                        }

                        @Override
                        public void addInt(int value) {
                          getMap(path).put(primitiveType.getName(), value);
                        }

                        @Override
                        public void addLong(long value) {
                          getMap(path).put(primitiveType.getName(), value);
                        }
                      };
                    }

                    @Override
                    public Converter convertGroupType(
                        List<GroupType> path, GroupType groupType, List<Converter> converters) {
                      return new GroupConverter() {

                        public Converter getConverter(int fieldIndex) {
                          return converters.get(fieldIndex);
                        }

                        public void start() {}

                        public void end() {}
                      };
                    }

                    @Override
                    public Converter convertMessageType(
                        MessageType messageType, List<Converter> children) {
                      log.info("convertMessageType called {} {} ", messageType, children);
                      return convertGroupType(null, messageType, children);
                    }
                  });
    }

    private Map<String, Object> getMap(List<GroupType> path) {
      Map<String, Object> res = record;
      if (path != null) {
        for (GroupType g : path) {
          if (!g.getName().equals("proxima-bulk")) {
            res.computeIfAbsent(g.getName(), name -> new HashMap<String, Map<String, Object>>());
            res = (Map<String, Object>) res.get(g.getName());
          }
        }
      }
      return res;
    }

    @Override
    public StreamElement getCurrentRecord() {
      final String key =
          (String) getRequiredValueFromCurrentRowData(ParquetFileFormat.PARQUET_COLUMN_NAME_KEY);
      final String operation =
          (String)
              getRequiredValueFromCurrentRowData(ParquetFileFormat.PARQUET_COLUMN_NAME_OPERATION);
      final String attributeName =
          (String)
              getRequiredValueFromCurrentRowData(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE);
      Optional<AttributeDescriptor<Object>> attribute = entity.findAttribute(attributeName);
      if (!attribute.isPresent()) {
        // current attribute is not in entity -> skip
        log.info(
            "Skipping attribute [{}] which is not in current attribute family.", attributeName);
        return null;
      }
      final String uuid =
          (String) getRequiredValueFromCurrentRowData(ParquetFileFormat.PARQUET_COLUMN_NAME_UUID);
      final long timestamp =
          (long)
              getRequiredValueFromCurrentRowData(ParquetFileFormat.PARQUET_COLUMN_NAME_TIMESTAMP);
      switch (OPERATION.of(operation)) {
        case DELETE:
          return StreamElement.delete(entity, attribute.get(), uuid, key, attributeName, timestamp);
        case DELETE_WILDCARD:
          return StreamElement.deleteWildcard(entity, attribute.get(), uuid, key, timestamp);
        case UPSERT:
          return StreamElement.upsert(
              entity,
              attribute.get(),
              uuid,
              key,
              attributeName,
              timestamp,
              getValueFromCurrentRowData(attribute.get()));
        default:
          throw new RecordMaterializationException("Unknown operation " + operation);
      }
    }

    private byte[] getValueFromCurrentRowData(AttributeDescriptor<?> attribute) {

      final String attributeKeyName = attributeNamesPrefix + attribute.toAttributePrefix(false);

      final SchemaTypeDescriptor<?> attributeSchema = attribute.getSchemaTypeDescriptor();
      Object value;
      if (attributeSchema.isStructureType()) {
        @SuppressWarnings("unchecked")
        final StructureValueAccessor<Object> valueAccessor =
            (StructureValueAccessor<Object>)
                attributeSchema.asStructureTypeDescriptor().getValueAccessor();
        value = valueAccessor.createFrom(record.get(attributeKeyName));
      } else if (attributeSchema.isArrayType()) {
        @SuppressWarnings("unchecked")
        final ArrayValueAccessor<Object> valueAccessor =
            (ArrayValueAccessor<Object>) attributeSchema.asArrayTypeDescriptor().getValueAccessor();
        // FIXME after bytes is resolved
        value = record.get(attributeKeyName);
        // value = valueAccessor.createFrom(record.get(attributeKeyName));
      } else {
        throw new UnsupportedOperationException("Fixme");
      }
      @SuppressWarnings("unchecked")
      final ValueSerializer<Object> serializer =
          (ValueSerializer<Object>) attribute.getValueSerializer();
      return serializer.serialize(value);
    }

    private Object getRequiredValueFromCurrentRowData(String column) {
      return Optional.ofNullable(record.getOrDefault(column, null))
          .orElseThrow(
              () ->
                  new IllegalStateException("Unable to read required value for column " + column));
    }

    @Override
    public GroupConverter getRootConverter() {
      return this.root;
    }
  }

  private static class BulkInputFile implements InputFile {

    private final SeekableByteChannel channel;

    BulkInputFile(SeekableByteChannel channel) {
      this.channel = channel;
    }

    @Override
    public long getLength() throws IOException {
      return channel.size();
    }

    @Override
    public SeekableInputStream newStream() {
      return new DelegatingSeekableInputStream(Channels.newInputStream(channel)) {

        @Override
        public long getPos() throws IOException {
          return channel.position();
        }

        @Override
        public void seek(long newPosition) throws IOException {
          channel.position(newPosition);
        }
      };
    }
  }
}
