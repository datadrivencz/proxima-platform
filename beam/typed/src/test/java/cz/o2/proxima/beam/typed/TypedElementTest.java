/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.typed;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.scheme.BytesSerializer;
import cz.o2.proxima.scheme.ValueSerializer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TypedElementTest {

  @Test
  public void testGetValue(@Mock AttributeDescriptor<byte[]> descriptor) {
    byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
    final ValueSerializer<byte[]> serializer = new BytesSerializer().getValueSerializer(null);
    when(descriptor.getValueSerializer()).thenReturn(serializer);
    final TypedElement<byte[]> element = TypedElement.upsert(descriptor, "key", value);
    element.clearCachedValue();
    element.getValue();
    element.getValue();
    // One serialization + two de-serializations.
    verify(descriptor, times(3)).getValueSerializer();
  }

  @Test
  public void testGetValueWithCache(@Mock AttributeDescriptor<byte[]> descriptor) {
    byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
    final ValueSerializer<byte[]> serializer = new BytesSerializer().getValueSerializer(null);
    when(descriptor.getValueSerializer()).thenReturn(serializer);
    final TypedElement<byte[]> element = TypedElement.upsert(descriptor, "key", value);
    element.clearCachedValue();
    element.getValue(true);
    element.getValue(true);
    // One serialization + one de-serializations.
    verify(descriptor, times(2)).getValueSerializer();
  }
}
