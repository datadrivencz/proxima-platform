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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.scheme.proto.test.Scheme;
import cz.o2.proxima.storage.StreamElement;
import java.time.Instant;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class RowDataResultExtractorTest {

  @Test
  void testToFlinkObjectCompatibilityWithDataType() {
    final Repository repository =
        Repository.ofTest(ConfigFactory.load("test-proto.conf").resolve());
    final EntityDescriptor entity = repository.getEntity("event");
    final AttributeDescriptor<Scheme.ValueSchemeMessage> attribute = entity.getAttribute("complex");
    final Scheme.ValueSchemeMessage message =
        Scheme.ValueSchemeMessage.newBuilder().setStringType("test string").setIntType(123).build();
    final StreamElement streamElement =
        StreamElement.upsert(
            entity,
            attribute,
            1L,
            "key",
            attribute.getName(),
            Instant.now().toEpochMilli(),
            attribute.getValueSerializer().serialize(message));
    final RowData rowData = (RowData) RowDataResultExtractor.toFlinkObject(streamElement);
    final DataType dataType = LogCatalogTable.toDataType(attribute.getSchemaTypeDescriptor());
    final RowType logicalType = (RowType) dataType.getLogicalType();
    assertEquals(
        message.getStringType(),
        rowData.getString(logicalType.getFieldIndex("string_type")).toString());
    assertEquals(message.getIntType(), rowData.getInt(logicalType.getFieldIndex("int_type")));
  }
}
