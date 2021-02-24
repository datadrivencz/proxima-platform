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

import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.bulk.Writer;
import cz.o2.proxima.direct.bulk.fs.parquet.ParquetFileFormat.OPERATION;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Optionals;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

@Slf4j
public class ProximaParquetWriter implements Writer {

  private final Path path;
  private final ParquetWriter<StreamElement> writer;

  public ProximaParquetWriter(
      Path path, MessageType schema, String attributeNamesPrefix, Configuration configuration)
      throws IOException {
    this.path = path;
    this.writer =
        new ParquetWriterBuilder(new BulkOutputFile(path.writer()), schema, attributeNamesPrefix)
            .withConf(configuration)
            .withWriteMode(Mode.OVERWRITE)
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

  private static class StreamElementWriteSupport extends WriteSupport<StreamElement> {

    private final MessageType parquetSchema;
    private final String attributeNamesPrefix;
    private StreamElementParquetWriter writer;

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
      this.writer =
          new StreamElementParquetWriter(recordConsumer, parquetSchema, attributeNamesPrefix);
    }

    @Override
    public void write(StreamElement record) {
      writer.write(record);
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
      writeStreamElementHeader(element);
      if (element.getValue() != null && element.getValue().length > 0) {
        writeAttribute(element);
      }
      recordConsumer.endMessage();
    }

    private void writeAttribute(StreamElement element) {
      log.trace("Writing stream element {}", element);
      String attribute =
          attributeNamesPrefix + element.getAttributeDescriptor().toAttributePrefix(false);
      SchemaTypeDescriptor<?> attributeSchema =
          schemasCache.computeIfAbsent(
              attribute,
              name ->
                  element.getAttributeDescriptor().getValueSerializer().getValueSchemaDescriptor());
      log.debug("Writing attribute {}", attribute);
      writeValue(attribute, attributeSchema, Optionals.get(element.getParsed()));
    }

    private <T> void writeValue(String name, SchemaTypeDescriptor<T> schema, T value) {
      log.debug("Writing field [{}] with schema [{}].", name, schema);
      writeStartField(name);
      switch (schema.getType()) {
        case STRUCTURE:
          recordConsumer.startGroup();
          final SchemaDescriptors.StructureTypeDescriptor<T> structureDescriptor =
              schema.getStructureTypeDescriptor();
          structureDescriptor
              .getFields()
              .forEach(
                  (field, type) -> {
                    @SuppressWarnings({"unchecked", "rawtypes "})
                    final SchemaTypeDescriptor<Object> cast = (SchemaTypeDescriptor) type;
                    writeValue(field, cast, structureDescriptor.readField(field, cast, value));
                  });
          recordConsumer.endGroup();
          break;
        case ARRAY:
          if (schema
              .getArrayTypeDescriptor()
              .getValueDescriptor()
              .getType()
              .equals(AttributeValueType.BYTE)) {
            // Array of bytes should be encoded just as binary
            recordConsumer.addBinary(Binary.fromReusedByteArray((byte[]) value));
          } else {
            throw new UnsupportedOperationException("Not implemented");
          }
          break;
        case BYTE:
          recordConsumer.addBinary(Binary.fromConstantByteArray(new byte[] {(byte) value}));
        case STRING:
          recordConsumer.addBinary(Binary.fromString(((String) value)));
        case ENUM:
          throw new UnsupportedOperationException("Not implemented.");
        case LONG:
          recordConsumer.addLong((long) value);
          break;
        case INT:
          recordConsumer.addInteger((int) value);
          break;
        case DOUBLE:
          recordConsumer.addDouble((double) value);
          break;
        case FLOAT:
          recordConsumer.addFloat((float) value);
          break;
        case BOOLEAN:
          recordConsumer.addBoolean((boolean) value);
          break;
      }
      writeEndField(name);
    }

    private void writeStartField(String name) {
      recordConsumer.startField(name, parquetSchema.getFieldIndex(name));
    }

    private void writeEndField(String name) {
      recordConsumer.endField(name, parquetSchema.getFieldIndex(name));
    }

    private void writeStreamElementHeader(StreamElement element) {
      writeStartField(ParquetFileFormat.PARQUET_COLUMN_NAME_KEY);
      recordConsumer.addBinary(Binary.fromString(element.getKey()));
      writeEndField(ParquetFileFormat.PARQUET_COLUMN_NAME_KEY);
      writeStartField(ParquetFileFormat.PARQUET_COLUMN_NAME_UUID);
      recordConsumer.addBinary(Binary.fromString(element.getUuid()));
      writeEndField(ParquetFileFormat.PARQUET_COLUMN_NAME_UUID);
      writeStartField(ParquetFileFormat.PARQUET_COLUMN_NAME_TIMESTAMP);
      recordConsumer.addLong(element.getStamp());
      writeEndField(ParquetFileFormat.PARQUET_COLUMN_NAME_TIMESTAMP);
      writeStartField(ParquetFileFormat.PARQUET_COLUMN_NAME_OPERATION);
      if (element.isDeleteWildcard()) {
        recordConsumer.addBinary(Binary.fromString(OPERATION.DELETE_WILDCARD.getValue()));
      } else if (element.isDelete()) {
        recordConsumer.addBinary(Binary.fromString(OPERATION.DELETE.getValue()));
      } else {
        recordConsumer.addBinary(Binary.fromString(OPERATION.UPSERT.getValue()));
      }
      writeEndField(ParquetFileFormat.PARQUET_COLUMN_NAME_OPERATION);
      writeStartField(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE);
      recordConsumer.addBinary(Binary.fromString(element.getAttribute()));
      writeEndField(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE);
      writeStartField(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX);
      recordConsumer.addBinary(
          Binary.fromString(element.getAttributeDescriptor().toAttributePrefix()));
      writeEndField(ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX);
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
