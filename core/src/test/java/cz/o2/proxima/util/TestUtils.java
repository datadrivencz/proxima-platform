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
package cz.o2.proxima.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class TestUtils {

  private TestUtils() {}

  /**
   * Check if object is serializable and return deserialized.
   *
   * @param object Object to check
   * @return object Deserialized object
   * @throws IOException
   */
  public static Object assertSerializable(Object object)
      throws IOException, ClassNotFoundException {
    Object deserialized = deserializeObject(serializeObject(object));
    assertEquals(
        String.format(
            "Deserialized object of class '%s' should be equals to input.",
            object.getClass().getName()),
        deserialized,
        object);
    return deserialized;
  }

  /**
   * Serialize object into bytes
   *
   * @param object object to serialize
   * @return byte[]
   * @throws IOException
   */
  public static byte[] serializeObject(Object object) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(buffer)) {
      oos.writeObject(object);
    }
    assertTrue(
        String.format("Class '%s' isn't serializable.", object.getClass().getName()),
        buffer.toByteArray().length > 0);
    return buffer.toByteArray();
  }

  /**
   * Deserialize object from bytes
   *
   * @param bytes
   * @return Object
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static Object deserializeObject(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      return ois.readObject();
    }
  }
}
