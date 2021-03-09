package cz.o2.proxima.direct.bulk.fs.parquet;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import cz.o2.proxima.direct.bulk.fs.parquet.InternalProximaRecordMaterializer.ParentValueContainer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

@Slf4j
class ProximaParquetRecordConverter extends GroupConverter {

  private final Converter[] converters;
  private final ParentValueContainer parent;

  public ProximaParquetRecordConverter(ParentValueContainer parent, GroupType schema) {
    Preconditions.checkNotNull(parent, "Missing parent value container.");
    Preconditions.checkNotNull(schema, "Missing schema.");
    this.parent = parent;
    int schemaSize = schema.getFieldCount();
    converters = new Converter[schemaSize];

    int parquetFieldIndex = 0;
    for (Type field : schema.getFields()) {
      converters[parquetFieldIndex++] = newConverter(parent, field, field.getName());
    }
  }

  private Converter newConverter(ParentValueContainer parentValueContainer, Type parquetType,
      String targetFieldName) {

    final LogicalTypeAnnotation logicalTypeAnnotation = parquetType.getLogicalTypeAnnotation();
    if (logicalTypeAnnotation == null) {
      if (parquetType.isPrimitive()) {
        return new ScalarConverter(parentValueContainer, targetFieldName);
      } else {
        // its structure
        log.warn("XXXXX {} {}", parquetType.getName(), targetFieldName);
        ParentValueContainer structureParent = null;
        if (parquetType.asGroupType().getRepetition().equals(Repetition.REPEATED)) {
          throw new UnsupportedOperationException("Repeated!");
        } else {
          parentValueContainer.add(targetFieldName, new HashMap<>());
          structureParent =
              new ParentValueContainer() {
                @Override
                @SuppressWarnings("unchecked")
                public void add(String name, Object value) {
                  if (parent.get(targetFieldName) instanceof List) {
                    // for repeated group set field into last
                    List<Map<String, Object>> list =
                        (List<Map<String, Object>>) parent.get(targetFieldName);
                    Iterables.getLast(list).put(name, value);
                  } else {
                    ((HashMap<String, Object>) parent.get(targetFieldName)).put(name, value);
                  }
                }

                @Override
                @SuppressWarnings("unchecked")
                public Object get(String name) {
                  if (parent.get(targetFieldName) instanceof List) {
                    // for repeated group set field into last
                    List<Map<String, Object>> list =
                        (List<Map<String, Object>>) parent.get(targetFieldName);
                    return Iterables.getLast(list);
                  } else {
                    return ((Map<String, Object>) parent.get(targetFieldName)).get(name);
                  }
                }
              };
        }
        return new ProximaParquetRecordConverter(structureParent, parquetType.asGroupType());
      }
    }
    return logicalTypeAnnotation
        .accept(
            new LogicalTypeAnnotationVisitor<Converter>() {
              @Override
              public Optional<Converter> visit(EnumLogicalTypeAnnotation enumLogicalType) {
                // enums is converted as string
                return Optional.of(new StringConverter(parent, targetFieldName));
              }

              @Override
              public Optional<Converter> visit(StringLogicalTypeAnnotation stringLogicalType) {
                return Optional.of(new StringConverter(parent, targetFieldName));
              }

              @Override
              public Optional<Converter> visit(ListLogicalTypeAnnotation listLogicalType) {
                return Optional.of(new ListConverter(parent, parquetType));
              }
            })
        .orElseGet(() -> new ScalarConverter(parent, parquetType.getName()));
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {}

  @Override
  public void end() {}

  private static class ScalarConverter extends PrimitiveConverter {

    private final ParentValueContainer parentValueContainer;
    private final String name;

    public ScalarConverter(ParentValueContainer parentValueContainer, String name) {
      this.parentValueContainer = parentValueContainer;
      this.name = name;
    }

    @Override
    public void addBinary(Binary value) {
      parentValueContainer.add(name, value.getBytes());
    }

    @Override
    public void addBoolean(boolean value) {
      parentValueContainer.add(name, value);
    }

    @Override
    public void addDouble(double value) {
      parentValueContainer.add(name, value);
    }

    @Override
    public void addFloat(float value) {
      parentValueContainer.add(name, value);
    }

    @Override
    public void addInt(int value) {
      parentValueContainer.add(name, value);
    }

    @Override
    public void addLong(long value) {
      parentValueContainer.add(name, value);
    }
  }

  private static class StringConverter extends PrimitiveConverter {

    private final ParentValueContainer parentValueContainer;
    private final String name;

    public StringConverter(ParentValueContainer parentValueContainer, String name) {
      this.parentValueContainer = parentValueContainer;
      this.name = name;
    }

    @Override
    public void addBinary(Binary value) {
      parentValueContainer.add(name, value.toStringUsingUTF8());
    }
  }

  private class ListConverter extends GroupConverter {
    private final Converter elementConverter;

    public ListConverter(ParentValueContainer parentValueContainer, Type parquetType) {

      LogicalTypeAnnotation logicalTypeAnnotation = parquetType.getLogicalTypeAnnotation();

      if (!(logicalTypeAnnotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation)
          || parquetType.isPrimitive()) {
        throw new ParquetDecodingException(
            "Expected LIST wrapper. Found: " + logicalTypeAnnotation + " instead.");
      }

      GroupType rootWrapperType = parquetType.asGroupType();
      if (!rootWrapperType.containsField("list") || rootWrapperType.getType("list").isPrimitive()) {
        throw new ParquetDecodingException(
            "Expected repeated 'list' group inside LIST wrapper but got: " + rootWrapperType);
      }

      GroupType listType = rootWrapperType.getType("list").asGroupType();
      if (!listType.containsField("element")) {
        throw new ParquetDecodingException(
            "Expected 'element' inside repeated list group but got: " + listType);
      }

      final Type elementType = listType.getType("element");
      parentValueContainer.add(parquetType.getName(), new ArrayList<>());
      final ParentValueContainer listParent =
          new ParentValueContainer() {
            @Override
            public void add(String name, Object value) {
              ((List<Object>) (parentValueContainer.get(parquetType.getName()))).add(value);
            }

            @Override
            public Object get(String name) {
              throw new UnsupportedOperationException("XXX");
            }
          };
      elementConverter = newConverter(listParent, elementType, parquetType.getName());
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex > 0) {
        throw new ParquetDecodingException("Unexpected multiple fields in the LIST wrapper");
      }
      return new GroupConverter() {
        @Override
        public Converter getConverter(int fieldIndex) {
          return elementConverter;
        }

        @Override
        public void start() {}

        @Override
        public void end() {}
      };
    }

    @Override
    public void start() {}

    @Override
    public void end() {}
  }
}
