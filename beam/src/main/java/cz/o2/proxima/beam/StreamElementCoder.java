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
package cz.o2.proxima.beam;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.commons.compress.utils.Charsets;

/**
 * Naive implementation of {@link org.apache.beam.sdk.coders.Coder} for {@link StreamElement}s.
 */
public class StreamElementCoder extends CustomCoder<StreamElement> {

  private static final Charset UTF8 = Charsets.UTF_8;

  private static enum Type {
    UPDATE,
    DELETE,
    DELETE_WILDCARD
  };

  /**
   * Create coder for StreamElements originating in given {@link Repository}.
   * @param repository the repository to create coder for
   * @return the coder
   */
  public static StreamElementCoder of(Repository repository) {
    return new StreamElementCoder(repository);
  }

  private final Repository repository;

  private StreamElementCoder(Repository repository) {
    this.repository = repository;
  }

  @Override
  public void encode(StreamElement value, OutputStream outStream)
      throws CoderException, IOException {
    final DataOutput output = new DataOutputStream(outStream);
    output.writeUTF(value.getEntityDescriptor().getName());
    output.writeUTF(value.getUuid());
    output.writeUTF(value.getKey());
    Type type = value.isDelete()
        ? value.isDeleteWildcard()
            ? Type.DELETE_WILDCARD
            : Type.DELETE
        : Type.UPDATE;
    output.writeInt(type.ordinal());
    String attribute = value.getAttribute();
    output.writeUTF(attribute == null
        ? value.getAttributeDescriptor().getName()
        : attribute);
    output.writeLong(value.getStamp());
    writeBytes(value.getValue(), output);
  }

  @Override
  public StreamElement decode(InputStream inStream) throws CoderException, IOException {
    final DataInput input = new DataInputStream(inStream);

    final String entityName = input.readUTF();
    final EntityDescriptor entityDescriptor = repository.findEntity(entityName)
        .orElseThrow(() -> new IOException("Unable to find entity " + entityName + "."));

    final String uuid = input.readUTF();
    final String key = input.readUTF();
    final int typeOrdinal = input.readInt();
    final Type type = Type.values()[typeOrdinal];
    String attributeName = input.readUTF();
    if (type.equals(Type.DELETE_WILDCARD)) {
        attributeName = attributeName.substring(0,attributeName.length()-1);
    }
    final String attribute = attributeName;

    AttributeDescriptor<Object> attributeDescriptor = entityDescriptor
        .findAttribute(attribute)
        .orElseThrow(() -> new IOException(
            "Unable to find attribute " + attribute + " of entity " + entityName));
    final long stamp = input.readLong();

    byte[] value = readBytes(input);
    switch (type) {
      case DELETE_WILDCARD:
        return StreamElement.deleteWildcard(
            entityDescriptor, attributeDescriptor, uuid, key, stamp);
      case DELETE:
        return StreamElement.delete(
            entityDescriptor, attributeDescriptor, uuid, key, attribute, stamp);
      case UPDATE:
        return StreamElement.update(
            entityDescriptor, attributeDescriptor, uuid,
            key, attribute, stamp, value);
    }
    throw new IllegalStateException("Unknown type " + type);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    // deterministic
  }

  private static void writeBytes(@Nullable byte[] value, DataOutput output)
      throws IOException {

    if (value == null) {
      output.writeInt(-1);
    } else {
      output.writeInt(value.length);
      output.write(value);
    }
  }

  private static @Nullable byte[] readBytes(DataInput input) throws IOException {
    int length = input.readInt();
    if (length >= 0) {
      byte[] ret = new byte[length];
      input.readFully(ret);
      return ret;
    }
    return null;
  }
}
