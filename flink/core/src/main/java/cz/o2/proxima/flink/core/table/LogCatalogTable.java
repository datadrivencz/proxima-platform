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
package cz.o2.proxima.flink.core.table;

import cz.o2.proxima.flink.core.FlinkDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.scheme.SchemaDescriptors;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.flink.shaded.guava18.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.VarCharType;

public abstract class LogCatalogTable<OptionsT extends FlinkDataOperator.LogOptions>
    implements org.apache.flink.table.catalog.CatalogTable {

  public static CommitLogCatalogTable ofCommitLog(
      Repository repository,
      List<AttributeDescriptor<?>> attributeDescriptors,
      FlinkDataOperator.CommitLogOptions logOptions) {
    return new CommitLogCatalogTable(repository, attributeDescriptors, logOptions);
  }

  public static BatchLogCatalogTable ofBatchLog(
      Repository repository,
      List<AttributeDescriptor<?>> attributeDescriptors,
      FlinkDataOperator.BatchLogOptions logOptions) {
    return new BatchLogCatalogTable(repository, attributeDescriptors, logOptions);
  }
  /**
   * Convert {@link SchemaDescriptors.SchemaTypeDescriptor} to Flink's {@link DataType}.
   *
   * @param schema Schema to convert.
   * @return Data type.
   */
  @VisibleForTesting
  static DataType toDataType(SchemaDescriptors.SchemaTypeDescriptor<?> schema) {
    switch (schema.getType()) {
      case INT:
        return DataTypes.INT();
      case BYTE:
        return DataTypes.BYTES();
      case ENUM:
        return DataTypes.STRING();
      case LONG:
        return DataTypes.BIGINT();
      case ARRAY:
        return DataTypes.ARRAY(toDataType(schema.asArrayTypeDescriptor().getValueDescriptor()));
      case FLOAT:
        return DataTypes.FLOAT();
      case DOUBLE:
        return DataTypes.DOUBLE();
      case STRING:
        return DataTypes.VARCHAR(VarCharType.MAX_LENGTH);
      case BOOLEAN:
        return DataTypes.BOOLEAN();
      case STRUCTURE:
        final DataTypes.Field[] fields =
            schema
                .asStructureTypeDescriptor()
                .getFields()
                .entrySet()
                .stream()
                .map(entry -> DataTypes.FIELD(entry.getKey(), toDataType(entry.getValue())))
                .toArray(DataTypes.Field[]::new);
        return DataTypes.ROW(fields);
      default:
        throw new IllegalArgumentException(
            String.format("Unknown data type %s.", schema.getType()));
    }
  }

  public static class CommitLogCatalogTable
      extends LogCatalogTable<FlinkDataOperator.CommitLogOptions> {

    public CommitLogCatalogTable(
        Repository repository,
        List<AttributeDescriptor<?>> attributeDescriptors,
        FlinkDataOperator.CommitLogOptions logOptions) {
      super(repository, attributeDescriptors, logOptions);
    }

    @Override
    public CatalogBaseTable copy() {
      return new CommitLogCatalogTable(getRepository(), getAttributeDescriptors(), getLogOptions());
    }
  }

  public static class BatchLogCatalogTable
      extends LogCatalogTable<FlinkDataOperator.BatchLogOptions> {

    public BatchLogCatalogTable(
        Repository repository,
        List<AttributeDescriptor<?>> attributeDescriptors,
        FlinkDataOperator.BatchLogOptions logOptions) {
      super(repository, attributeDescriptors, logOptions);
    }

    @Override
    public CatalogBaseTable copy() {
      return new BatchLogCatalogTable(getRepository(), getAttributeDescriptors(), getLogOptions());
    }
  }

  @Getter private final Repository repository;
  @Getter private final List<AttributeDescriptor<?>> attributeDescriptors;
  @Getter private final OptionsT logOptions;

  public LogCatalogTable(
      Repository repository,
      List<AttributeDescriptor<?>> attributeDescriptors,
      OptionsT logOptions) {
    this.repository = repository;
    this.attributeDescriptors = attributeDescriptors;
    this.logOptions = logOptions;
  }

  @Override
  public boolean isPartitioned() {
    return false;
  }

  @Override
  public List<String> getPartitionKeys() {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public org.apache.flink.table.catalog.CatalogTable copy(Map<String, String> map) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public Map<String, String> getOptions() {
    final Map<String, String> options = new HashMap<>();
    options.put("connector", LogDynamicTableSourceFactory.FACTORY_IDENTIFIER);
    return options;
  }

  @Override
  public Schema getUnresolvedSchema() {
    final Schema.Builder schemaBuilder =
        Schema.newBuilder()
            .column("key", DataTypes.VARCHAR(VarCharType.MAX_LENGTH).notNull())
            .column("uuid", DataTypes.VARCHAR(VarCharType.MAX_LENGTH).nullable())
            .column("attribute", DataTypes.VARCHAR(VarCharType.MAX_LENGTH).notNull())
            .column("attribute_prefix", DataTypes.VARCHAR(VarCharType.MAX_LENGTH).notNull())
            .column("timestamp", DataTypes.TIMESTAMP(3).notNull())
            .primaryKey("key")
            .watermark("timestamp", "SOURCE_WATERMARK()");
    for (AttributeDescriptor<?> attributeDescriptor : attributeDescriptors) {
      schemaBuilder.column(
          attributeDescriptor.toAttributePrefix(false),
          toDataType(attributeDescriptor.getSchemaTypeDescriptor()));
    }
    return schemaBuilder.build();
  }

  @Override
  public String getComment() {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public Optional<String> getDescription() {
    return Optional.empty();
  }

  @Override
  public Optional<String> getDetailedDescription() {
    return Optional.empty();
  }
}
