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
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

/**
 * Naive implementation of {@link org.apache.beam.sdk.coders.Coder} for {@link StreamElement}s.
 */
public class StreamElementCoder extends CustomCoder<StreamElement> {

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
    output.writeUTF(value.getAttributeDescriptor().getName());
    output.writeUTF(value.getUuid());
    output.writeUTF(value.getKey());
    // FIXME (je-ik)
    output.writeUTF(Objects.requireNonNull(value.getAttribute()));
    output.writeLong(value.getStamp());
    if (value.isDelete()) {
      output.writeInt(-1);
    } else {
      output.writeInt(Objects.requireNonNull(value.getValue()).length);
      output.write(value.getValue());
    }
  }

  @Override
  public StreamElement decode(InputStream inStream) throws CoderException, IOException {
    final DataInputStream input = new DataInputStream(inStream);

    final String entityName = input.readUTF();
    final EntityDescriptor entityDescriptor = repository.findEntity(entityName)
        .orElseThrow(() -> new IOException("Unable to find entity " + entityName + "."));

    final String attributeName = input.readUTF();
    final AttributeDescriptor attributeDescriptor = entityDescriptor.findAttribute(attributeName)
        .orElseThrow(() -> new IOException("Unable to find attribute " + attributeName + "."));

    final String uuid = input.readUTF();
    final String key = input.readUTF();
    final String attribute = input.readUTF();
    final long stamp = input.readLong();

    final int valueLength = input.readInt();
    if (valueLength == -1) {
      // delete
      return StreamElement.delete(
          entityDescriptor, attributeDescriptor, uuid, key, attribute, stamp);
    } else {
      final byte[] value = new byte[valueLength];
      if (valueLength != input.read(value, 0, valueLength)) {
        throw new IOException("Premature end of input stream.");
      }
      return StreamElement.update(
          entityDescriptor, attributeDescriptor, uuid, key, attribute, stamp, value);
    }
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    // deterministic
  }
}
