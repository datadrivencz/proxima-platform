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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.protobuf.ByteString;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.bulk.AbstractFileFormatTest;
import cz.o2.proxima.direct.bulk.FileFormat;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.bulk.Reader;
import cz.o2.proxima.direct.bulk.Writer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.scheme.proto.test.Scheme.Event;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.Directions;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.InnerMessage;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.TestUtils;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Slf4j
@RunWith(Parameterized.class)
public class ParquetFileFormatTest extends AbstractFileFormatTest {

  private final AttributeDescriptor<byte[]> notFromFamilyAttribute =
      AttributeDescriptor.newBuilder(repo)
          .setEntity("entity")
          .setName("another-attribute")
          .setSchemeUri(new URI("bytes:///"))
          .build();

  @Parameterized.Parameter public TestParams testParams;

  public ParquetFileFormatTest() throws URISyntaxException {}

  @Parameterized.Parameters
  public static Collection<TestParams> params() {
    return Arrays.asList(
        TestParams.builder().expectedSuffix("").blockSize(100).build(),
        TestParams.builder().gzip(true).expectedSuffix(".gz").build(),
        TestParams.builder().gzip(false).expectedSuffix("").build(),
        TestParams.builder()
            .compression(CompressionCodecName.GZIP.name())
            .expectedSuffix(".gz")
            .build(),
        TestParams.builder()
            .compression(CompressionCodecName.SNAPPY.name())
            .expectedSuffix(".snappy")
            .build());
  }

  private Map<String, Object> getCfg() {
    Map<String, Object> cfg =
        new HashMap<String, Object>() {
          {
            put("gzip", testParams.gzip);
          }
        };
    if (testParams.getBlockSize() > 0) {
      cfg.put(ParquetFileFormat.PARQUET_CONFIG_PAGE_SIZE_KEY_NAME, testParams.getBlockSize());
    }
    if (testParams.getCompression() != null) {
      cfg.put(ParquetFileFormat.PARQUET_CONFIG_COMPRESSION_KEY_NAME, testParams.getCompression());
    }
    return cfg;
  }

  @Override
  protected FileFormat getFileFormat() {
    FileFormat format = new ParquetFileFormat();
    format.setup(
        TestUtils.createTestFamily(
            entity, URI.create("test:///"), Arrays.asList(attribute, wildcard), getCfg()));
    return format;
  }

  @Test
  public void testFileExtension() {
    assertEquals("parquet" + testParams.getExpectedSuffix(), getFileFormat().fileSuffix());
    assertEquals(
        testParams.getExpectedSuffix(),
        ((ParquetFileFormat) getFileFormat()).parquetCompressionCodec.getExtension());
  }

  @Test
  public void testParquetWriterSettings() {
    ParquetFileFormat format = (ParquetFileFormat) getFileFormat();
    int blockSize =
        format
            .createWriterConfiguration()
            .getInt(
                ParquetFileFormat.PARQUET_CONFIG_PAGE_SIZE_KEY_NAME,
                ParquetFileFormat.PARQUET_DEFAULT_PAGE_SIZE);
    if (testParams.blockSize > 0) {
      assertEquals(testParams.blockSize, blockSize);
    } else {
      assertEquals(ParquetFileFormat.PARQUET_DEFAULT_PAGE_SIZE, blockSize);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWriteAttributeNotFromFamilyShouldThrowsException() throws IOException {
    writeElements(
        file,
        getFileFormat(),
        entity,
        Collections.singletonList(
            StreamElement.upsert(
                entity,
                notFromFamilyAttribute,
                UUID.randomUUID().toString(),
                "key",
                notFromFamilyAttribute.toAttributePrefix(),
                now,
                new byte[] {1})));
  }

  @Test
  public void testAttributesNotFromFamilyShouldBeSkipped() throws IOException {
    URL testResource =
        getClass().getClassLoader().getResource("stored-attribute-not-in-family.parquet");
    assertNotNull("Unable to read test parquet file from resources", testResource);
    File file = new File(testResource.getPath());
    Path path =
        Path.local(
            FileSystem.local(
                file,
                NamingConvention.defaultConvention(
                    Duration.ofHours(1), "prefix", getFileFormat().fileSuffix())),
            file);

    List<StreamElement> elements = readElements(path, getFileFormat(), entity);
    assertEquals(1, elements.size()); // we have 2 elements in test file where just one is valid
  }

  @Test
  public void testWriteAndReadComplexValue() throws IOException {
    final Repository protoRepo =
        ConfigRepository.ofTest(ConfigFactory.load("test-proto.conf").resolve());
    final EntityDescriptor event = protoRepo.getEntity("event");
    final AttributeDescriptor<Event> dataAttribute = event.getAttribute("data");
    List<StreamElement> elements =
        Arrays.asList(
            StreamElement.upsert(
                event,
                dataAttribute,
                UUID.randomUUID().toString(),
                "key1",
                dataAttribute.getName(),
                now,
                Event.newBuilder()
                    .setGatewayId("gatewayId1")
                    .setPayload(ByteString.copyFromUtf8("payload1"))
                    .build()
                    .toByteArray()),
            StreamElement.upsert(
                event,
                dataAttribute,
                UUID.randomUUID().toString(),
                "key2",
                dataAttribute.getName(),
                now,
                Event.newBuilder()
                    .setGatewayId("gatewayId2")
                    .setPayload(ByteString.copyFromUtf8("payload2"))
                    .build()
                    .toByteArray()));
    FileFormat format = getFileFormat();
    format.setup(
        TestUtils.createTestFamily(
            entity, URI.create("test:///"), Collections.singletonList(dataAttribute), getCfg()));

    assertWriteAndReadElements(format, event, elements);
  }

  @Test
  public void testWriteProtoBufComplexObject() throws IOException {
    final Repository complexRepo =
        Repository.ofTest(ConfigFactory.load("test-proto.conf").resolve());
    final EntityDescriptor event = complexRepo.getEntity("event");
    final AttributeDescriptor<ValueSchemeMessage> complexAttr = event.getAttribute("complex");

    final FileFormat fileFormat = getFileFormat();
    fileFormat.setup(
        TestUtils.createTestFamily(
            event,
            URI.create("test:///"),
            Collections.singletonList(complexAttr),
            Collections.emptyMap()));

    assertWriteAndReadElements(
        fileFormat,
        event,
        Collections.singletonList(
            StreamElement.upsert(
                event,
                complexAttr,
                UUID.randomUUID().toString(),
                "key1",
                complexAttr.getName(),
                now,
                complexAttr
                    .getValueSerializer()
                    .serialize(
                        ValueSchemeMessage.newBuilder()
                            .addRepeatedString("repeated_string_value_1")
                            .addRepeatedString("repeated_string_value_2")
                            .setInnerMessage(
                                InnerMessage.newBuilder()
                                    .setInnerEnum(Directions.LEFT)
                                    .setInnerDoubleType(69)
                                    .addRepeatedInnerString("bar")
                                    .addRepeatedInnerString("bar2")
                                    .build())
                            .addRepeatedInnerMessage(
                                InnerMessage.newBuilder()
                                    .addRepeatedInnerString("foo")
                                    .addRepeatedInnerString("bar")
                                    .setInnerDoubleType(33)
                                    .build())
                            .addRepeatedInnerMessage(
                                InnerMessage.newBuilder()
                                    .addRepeatedInnerString("foo2")
                                    .addRepeatedInnerString("bar2")
                                    .setInnerDoubleType(66)
                                    .build())
                            .setIntType(10)
                            .setBooleanType(false)
                            .build()))));
  }

  @Test
  public void testReadComplex() throws IOException {
    File file = new File("/tmp/complex-object.parquet");
    Path path =
        Path.local(
            FileSystem.local(
                file,
                NamingConvention.defaultConvention(
                    Duration.ofHours(1), "prefix", getFileFormat().fileSuffix())),
            file);
    try (Reader reader = getFileFormat().openReader(path, entity)) {

      for (StreamElement e : reader) {
        log.warn("Get: {}", e);
      }
    }
  }

  @Test
  @Ignore(value = "to be deleted")
  public void x() throws IOException {
    File file = new File("/tmp/file.parquet");
    Path path =
        Path.local(
            FileSystem.local(
                file,
                NamingConvention.defaultConvention(
                    Duration.ofHours(1), "prefix", getFileFormat().fileSuffix())),
            file);
    try (Writer writer = getFileFormat().openWriter(path, entity)) {

      writer.write(
          StreamElement.upsert(
              entity,
              notFromFamilyAttribute,
              UUID.randomUUID().toString(),
              "key",
              notFromFamilyAttribute.toAttributePrefix(),
              now,
              new byte[] {69}));

      writer.write(upsert());
    }
  }

  @Builder
  @Value
  private static class TestParams {
    boolean gzip;
    String compression;
    String expectedSuffix;
    int blockSize;
  }
}
