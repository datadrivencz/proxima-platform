package cz.o2.proxima.beam;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Optionals;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.Test;

public class StreamElementCoderTest {

  private static final String ENTITY = "event";
  private static final String ATTRIBUTE = "data";
  private static final String KEY = "key";
  private static final long STAMP = 123L;
  private static final byte[] VALUE = "value".getBytes(StandardCharsets.UTF_8);


  @Test
  public void testUpdate() throws IOException {

    final Repository repository = Repository.of(ConfigFactory.load("test-reference.conf"));

    final EntityDescriptor entityDescriptor = Optionals.get(repository.findEntity(ENTITY));

    final AttributeDescriptor attributeDescriptor =
        Optionals.get(entityDescriptor.findAttribute(ATTRIBUTE));

    final String uuid = UUID.randomUUID().toString();

    final StreamElement update = StreamElement.update(
        entityDescriptor, attributeDescriptor, uuid, KEY, ATTRIBUTE, STAMP, VALUE);

    final StreamElementCoder coder = StreamElementCoder.of(repository);

    // encode
    final byte[] buf;
    try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      coder.encode(update, outputStream);
      buf = outputStream.toByteArray();
    }

    // decode
    final StreamElement decoded;
    try (final InputStream inputStream = new ByteArrayInputStream(buf)) {
      decoded = coder.decode(inputStream);
    }

    assertNotNull(decoded);
    assertEquals(entityDescriptor, decoded.getEntityDescriptor());
    assertEquals(attributeDescriptor, decoded.getAttributeDescriptor());
    assertEquals(uuid, decoded.getUuid());
    assertEquals(KEY, decoded.getKey());
    assertEquals(ATTRIBUTE, decoded.getAttribute());
    assertEquals(STAMP, decoded.getStamp());
    assertArrayEquals(VALUE, decoded.getValue());
  }
}
