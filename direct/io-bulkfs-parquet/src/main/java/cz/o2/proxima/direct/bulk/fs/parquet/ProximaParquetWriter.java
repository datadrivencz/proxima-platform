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

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.bulk.Writer;
import cz.o2.proxima.direct.bulk.fs.parquet.ParquetFileFormat.OPERATION;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValueAccessor;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors.ArrayTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Optionals;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

@Slf4j
public class ProximaParquetWriter implements Writer {

  private final Path path;
  private final ParquetWriter<StreamElement> writer;

  public ProximaParquetWriter(
      Path path, MessageType schema, String attributeNamesPrefix,
      CompressionCodecName compressionCodecName, Configuration config)
      throws IOException {
    this.path = path;
    this.writer =
        new ParquetWriterBuilder(new BulkOutputFile(path.writer()), schema, attributeNamesPrefix)
            .withConf(config)
            .withWriteMode(Mode.OVERWRITE)
            // For some reason Writer ignores settings this via withConf()
            .withCompressionCodec(compressionCodecName)
            .withRowGroupSize(
                config.getInt(
                    ParquetFileFormat.PARQUET_CONFIG_PAGE_SIZE_KEY_NAME,
                    ParquetFileFormat.PARQUET_DEFAULT_PAGE_SIZE))
            .withPageSize(
                config.getInt(
                    ParquetFileFormat.PARQUET_CONFIG_PAGE_SIZE_KEY_NAME,
                    ParquetFileFormat.PARQUET_DEFAULT_PAGE_SIZE))
            .withMaxPaddingSize(
                config.getInt(
                    ParquetFileFormat.PARQUET_CONFIG_MAX_PADDING_BYTES_KEY_NAME,
                    ParquetFileFormat.PARQUET_DEFAULT_MAX_PADDING_BYTES))
            .build();
  }

  @Override
  public void write(StreamElement elem) throws IOException {
    try {
      writer.write(elem);
    } catch (InvalidRecordException iex) {
      throw new IllegalArgumentException("Unable to write StreamElement.", iex);
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public Path getPath() {
    return path;
  }

  private static class ParquetWriterBuilder
      extends ParquetWriter.Builder<StreamElement, ParquetWriterBuilder> {

    private final MessageType parquetSchema;
    private final String attributeNamesPrefix;

    private ParquetWriterBuilder(
        OutputFile outputFile, MessageType parquetSchema, String attributeNamesPrefix) {
      super(outputFile);
      this.parquetSchema = parquetSchema;
      this.attributeNamesPrefix = attributeNamesPrefix;
    }

    @Override
    protected ParquetWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<StreamElement> getWriteSupport(Configuration conf) {
      return new StreamElementWriteSupport(parquetSchema, attributeNamesPrefix);
    }
  }

  static class StreamElementWriteSupport extends WriteSupport<StreamElement> {

    private final MessageType parquetSchema;
    private final String attributeNamesPrefix;
    private StreamElementParquetWriter writer;
    private RecordConsumer recordConsumer;

    public StreamElementWriteSupport(MessageType parquetSchema, String attributeNamesPrefix) {
      this.parquetSchema = parquetSchema;
      this.attributeNamesPrefix = attributeNamesPrefix;
    }

    @Override
    public WriteContext init(Configuration configuration) {
      return new WriteContext(parquetSchema, Collections.emptyMap());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
      this.recordConsumer = recordConsumer;
      this.writer =
          new StreamElementParquetWriter(recordConsumer, parquetSchema, attributeNamesPrefix);
    }

    @Override
    public void write(StreamElement record) {
      writer.write(record);
    }

    @VisibleForTesting
    void injectSchemaCache(String name, SchemaTypeDescriptor<?> schema) {
      writer.schemasCache.put(name, schema);
    }

    FieldWriter<?> createWriter(SchemaTypeDescriptor<?> schema) {
      switch (schema.getType()) {
        case STRING:
          return new StringWriter();
        case INT:
          return new IntWriter();
        case LONG:
          return new LongWriter();
        case FLOAT:
          return new FloatWriter();
        case DOUBLE:
          return new DoubleWriter();
        case ARRAY:
          final ArrayTypeDescriptor<?> arrayDescriptor = schema.asArrayTypeDescriptor();
          if (arrayDescriptor.getValueDescriptor().getType().equals(AttributeValueType.BYTE)) {
            return new BytesWriter();
          } else {
            return new ArrayWriter<>(
                (FieldWriter<?>) createWriter(arrayDescriptor.getValueDescriptor()));
          }
        case STRUCTURE:
          return new StructureWriter<>(schema.asStructureTypeDescriptor());
        default:
          throw new InvalidRecordException(
              String.format("Unable to create writer for unknown type [%s]", schema.getType()));
      }
    }

    interface FieldWriter<T> {

      void setName(String name);

      void setIndex(int index);

      default void writeRawValue(T value) {
        throw new UnsupportedOperationException("Method writeRawValue should be overridden.");
      }

      void writeField(T value);
    }

    abstract class GenericFieldWriter<T> implements FieldWriter<T> {

      @Setter
      String name;
      @Setter
      int index = -1;

      public void writeField(T value) {
        recordConsumer.startField(name, index);
        writeRawValue(value);
        recordConsumer.endField(name, index);
      }
    }

    class StringWriter extends GenericFieldWriter<String> {

      @Override
      public void writeRawValue(String value) {
        recordConsumer.addBinary(Binary.fromString(value));
      }
    }

    class IntWriter extends GenericFieldWriter<Integer> {

      @Override
      public void writeRawValue(Integer value) {
        recordConsumer.addInteger(value);
      }
    }

    class LongWriter extends GenericFieldWriter<Long> {

      @Override
      public void writeRawValue(Long value) {
        recordConsumer.addLong(value);
      }
    }

    class FloatWriter extends GenericFieldWriter<Float> {

      @Override
      public void writeRawValue(Float value) {
        recordConsumer.addFloat(value);
      }
    }

    class DoubleWriter extends GenericFieldWriter<Double> {

      @Override
      public void writeRawValue(Double value) {
        recordConsumer.addDouble(value);
      }
    }

    class BytesWriter extends GenericFieldWriter<byte[]> {

      @Override
      public void writeRawValue(byte[] value) {
        recordConsumer.addBinary(Binary.fromReusedByteArray(value));
      }
    }

    class ArrayWriter<T> extends GenericFieldWriter<List<T>> {

      final FieldWriter<T> fieldWriter;

      ArrayWriter(
          FieldWriter<T> fieldWriter) {
        this.fieldWriter = fieldWriter;
      }

      @Override
      public void writeField(List<T> value) {
        recordConsumer.startGroup();
        recordConsumer.startField("list", 0); // This is the wrapper group for the array field
        value.forEach(val -> {
          recordConsumer.startGroup();
          recordConsumer.startField("element", 0); // This is the mandatory inner field

          fieldWriter.writeRawValue(val);

          recordConsumer.endField("element", 0);
          recordConsumer.endGroup();
        });
        recordConsumer.endField("list", 0);
        recordConsumer.endGroup();
      }
    }

    class StructureWriter<T> extends GenericFieldWriter<T> {

      final FieldWriter<?>[] fieldWriters;

      public StructureWriter(StructureTypeDescriptor<T> schema) {
        Map<String, SchemaTypeDescriptor<?>> fields = schema.getFields();
        fieldWriters = (FieldWriter<?>[]) Array
            .newInstance(GenericFieldWriter.class, fields.size());
        int index = 0;
        for (Entry<String, SchemaTypeDescriptor<?>> field : fields.entrySet()) {
          final String name = field.getKey();
          final SchemaTypeDescriptor<?> typeDescriptor = field.getValue();
          final FieldWriter<?> fieldWriter = createWriter(typeDescriptor);
          fieldWriter.setName(name);
          fieldWriter.setIndex(index);
          fieldWriters[index++] = fieldWriter;
        }
      }

      @Override
      public void writeRawValue(T value) {
        recordConsumer.startGroup();
        writeFields(value);
        recordConsumer.endGroup();
        ;
      }

      private void writeFields(T value) {

      }
    }
  }

  private static class StreamElementParquetWriter {

    private final RecordConsumer recordConsumer;
    private final MessageType parquetSchema;
    private final String attributeNamesPrefix;
    private final Map<String, SchemaTypeDescriptor<?>> schemasCache = new HashMap<>();

    public StreamElementParquetWriter(
        RecordConsumer recordConsumer, MessageType parquetSchema, String attributeNamesPrefix) {
      this.recordConsumer = recordConsumer;
      this.parquetSchema = parquetSchema;
      this.attributeNamesPrefix = attributeNamesPrefix;
    }

    public void write(StreamElement element) {
      recordConsumer.startMessage();
      writeStreamElementHeader(element, parquetSchema);
      if (element.getValue() != null && element.getValue().length > 0) {
        writeAttribute(element);
      }
      recordConsumer.endMessage();
    }

    private void writeAttribute(StreamElement element) {
      log.debug("Writing stream element {}", element);
      String attribute =
          attributeNamesPrefix + element.getAttributeDescriptor().toAttributePrefix(false);
      SchemaTypeDescriptor<?> attributeSchema =
          schemasCache.computeIfAbsent(
              attribute,
              name ->
                  element.getAttributeDescriptor().getValueSerializer().getValueSchemaDescriptor());
      log.debug("Writing attribute {}", attribute);
      writeValue(
          attribute, attributeSchema, Optionals.get(element.getParsed()), parquetSchema, true);
    }

    private <T> void writeValue(
        String name,
        SchemaTypeDescriptor<T> schema,
        T value,
        GroupType currentParquetSchema,
        boolean writeStartAndEndField) {
      if (writeStartAndEndField) {
        writeStartField(name, currentParquetSchema);
      }
      log.debug("Writing field [{}] with schema [{}].", name, schema);
      switch (schema.getType()) {
        case STRUCTURE:
          StructureValueAccessor<Object> valueAccessor = (StructureValueAccessor<Object>) schema
              .asStructureTypeDescriptor()
              .getValueAccessor();
          GroupType innerSchema =
              currentParquetSchema
                  .getFields()
                  .stream()
                  .filter(attr -> attr.getName().equals(name))
                  .findFirst()
                  .map(Type::asGroupType)
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              String.format(
                                  "Unable to find attribute [%s] in parquet schema [%s].",
                                  name, currentParquetSchema)));
          if (innerSchema.getLogicalTypeAnnotation() != null && innerSchema
              .getLogicalTypeAnnotation().equals(
                  LogicalTypeAnnotation.listType())) {
            innerSchema = innerSchema.asGroupType().getType("list").asGroupType().getType("element")
                .asGroupType();
          }
          final Map<String, Object> structureValues = new HashMap<>();
          try {
            valueAccessor.valuesOf(value).forEach(structureValues::put);
          } catch (Exception e) {
            ((Map<String, Object>) value).forEach(structureValues::put);
          }
          recordConsumer.startGroup();
          final GroupType finalInnerSchema = innerSchema;
          schema
              .asStructureTypeDescriptor()
              .getFields()
              .forEach(
                  (field, type) -> {
                    /*
                    if ((!structureValues.get(field).toString().isEmpty() && !type.isArrayType())
                        || (type.isArrayType() && !type.asArrayTypeDescriptor()
                        .getValueDescriptor().getType().equals(AttributeValueType.BYTE)
                        && ((Object[]) structureValues.get(field)).length > 0)) {

                     */
                      writeValue(
                          field,
                          (SchemaTypeDescriptor<Object>) type,
                          structureValues.get(field),
                          finalInnerSchema,
                          true);
                    //}
                  });
          recordConsumer.endGroup();
          break;
          /*
          recordConsumer.startGroup();
          final SchemaDescriptors.StructureTypeDescriptor<T> structureDescriptor =
              schema.asStructureTypeDescriptor();
          final Type innerSchema =
              currentParquetSchema
                  .getFields()
                  .stream()
                  .filter(attr -> attr.getName().equals(name))
                  .findFirst()
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              String.format(
                                  "Unable to find attribute [%s] in parquet schema [%s].",
                                  name, currentParquetSchema)));
          structureDescriptor
              .getFields()
              .forEach(
                  (field, type) -> {
                    @SuppressWarnings("unchecked")
                    SchemaTypeDescriptor<Object> cast =
                        (SchemaTypeDescriptor<Object>) type;
                    final Object fieldValue = structureDescriptor.getValueAccessor()
                        .readField(field, value);
                    boolean isEmptyValue = false;

                    /**
                     * FIXME: This is dirty magic which checked if value is provided If not, value
                     * is not written. This should be delegated into AttributeValueAccessor and
                     * should not be here!
                     *
                    if (type.isArrayType() && !type.asArrayTypeDescriptor().getValueDescriptor().getType().equals(AttributeValueType.BYTE) && ((Object[]) fieldValue).length > 0) {
                      isEmptyValue = true;
                    } else if (type.isStructureType()
                        && fieldValue.toString().isEmpty()) {
                      isEmptyValue = true;
                    }

                    if (!isEmptyValue) {
                      writeValue(field, cast, fieldValue, innerSchema.asGroupType(), true);
                    }
                  });
          recordConsumer.endGroup();
          break;

           */
        case ARRAY:
          final SchemaTypeDescriptor<T> valueDescriptor =
              schema.asArrayTypeDescriptor().getValueDescriptor();
          if (valueDescriptor.getType().equals(AttributeValueType.BYTE)) {
            // Array of bytes should be encoded just as binary
            recordConsumer.addBinary(Binary.fromReusedByteArray((byte[]) value));
          } else {
            T[] values = schema.asArrayTypeDescriptor().getValueAccessor().valuesOf(value);
            if (values.length > 0) {
              recordConsumer.startGroup();
              recordConsumer.startField("list", 0); // This is the wrapper group for the array field
              for (T val : values) {
                recordConsumer.startGroup();
                recordConsumer.startField("element", 0); // This is the mandatory inner field

                writeValue(name, valueDescriptor, val, currentParquetSchema, false);

                recordConsumer.endField("element", 0);
                recordConsumer.endGroup();
              }
              recordConsumer.endField("list", 0);

              recordConsumer.endGroup();
            }
          }
          break;
        case BYTE:
          recordConsumer.addBinary(Binary.fromConstantByteArray(new byte[] {(Byte) value}));
          break;
        case STRING:
        case ENUM:
          recordConsumer.addBinary(Binary.fromString((String) value));
          break;
        case LONG:
          recordConsumer.addLong((Long) value);
          break;
        case INT:
          recordConsumer.addInteger((Integer) value);
          break;
        case DOUBLE:
          recordConsumer.addDouble((Double) value);
          break;
        case FLOAT:
          recordConsumer.addFloat((Float) value);
          break;
        case BOOLEAN:
          recordConsumer.addBoolean((Boolean) value);
          break;
        default:
          throw new IllegalArgumentException("Unable to write unknown type " + schema.getType());
      }
      if (writeStartAndEndField) {
        writeEndField(name, currentParquetSchema);
      }
    }

    private void writeStartField(String name, GroupType schema) {
      log.debug("writing start field {} of schema {}", name, schema.getName());
      recordConsumer.startField(name, schema.getFieldIndex(name));
    }

    private void writeEndField(String name, GroupType schema) {
      log.debug("writing end field {} of schema {}", name, schema.getName());
      recordConsumer.endField(name, schema.getFieldIndex(name));
    }

    private void writeStreamElementHeader(StreamElement element, GroupType schema) {
      writeStartField(ParquetFileFormat.PARQUET_COLUMN_NAME_KEY, schema);
      recordConsumer.addBinary(Binary.fromString(element.getKey()));
      writeEndField(ParquetFileFormat.PARQUET_COLUMN_NAME_KEY, schema);
      writeStartField(ParquetFileFormat.PARQUET_COLUMN_NAME_UUID, schema);
      recordConsumer.addBinary(Binary.fromString(element.getUuid()));
      writeEndField(ParquetFileFormat.PARQUET_COLUMN_NAME_UUID, schema);
      writeStartField(ParquetFileFormat.PARQUET_COLUMN_NAME_TIMESTAMP, schema);
      recordConsumer.addLong(element.getStamp());
      writeEndField(ParquetFileFormat.PARQUET_COLUMN_NAME_TIMESTAMP, schema);
      writeStartField(ParquetFileFormat.PARQUET_COLUMN_NAME_OPERATION, schema);
      if (element.isDeleteWildcard()) {
        recordConsumer.addBinary(Binary.fromString(OPERATION.DELETE_WILDCARD.getValue()));
      } else if (element.isDelete()) {
        recordConsumer.addBinary(Binary.fromString(OPERATION.DELETE.getValue()));
      } else {
        recordConsumer.addBinary(Binary.fromString(OPERATION.UPSERT.getValue()));
      }
      writeEndField(ParquetFileFormat.PARQUET_COLUMN_NAME_OPERATION, schema);
      writeStartField(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE, schema);
      recordConsumer.addBinary(Binary.fromString(element.getAttribute()));
      writeEndField(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE, schema);
      writeStartField(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX, schema);
      recordConsumer.addBinary(
          Binary.fromString(element.getAttributeDescriptor().toAttributePrefix()));
      writeEndField(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX, schema);
    }
  }

  private static class BulkOutputFile implements OutputFile {

    private final OutputStream outputStream;

    BulkOutputFile(OutputStream outputStream) {
      this.outputStream = outputStream;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      return new BulkOutputStream(outputStream);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      return new BulkOutputStream(outputStream);
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0;
    }
  }

  private static class BulkOutputStream extends PositionOutputStream {

    private final OutputStream delegate;
    private long position = 0;

    private BulkOutputStream(OutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public void write(int b) throws IOException {
      position++;
      delegate.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      delegate.write(b, off, len);
      position += len;
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
