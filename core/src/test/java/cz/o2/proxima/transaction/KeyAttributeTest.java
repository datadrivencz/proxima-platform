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
package cz.o2.proxima.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import org.junit.Test;

public class KeyAttributeTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
  private final AttributeDescriptor<byte[]> device = gateway.getAttribute("device.*");

  @Test
  public void testKeyAttributeConstruction() {
    KeyAttribute ka = KeyAttribute.ofAttributeDescriptor(gateway, "gw", status);
    KeyAttribute ka2 = KeyAttribute.ofAttributeDescriptor(gateway, "gw", status);
    assertEquals(ka, ka2);
    ka = KeyAttribute.ofAttributeDescriptor(gateway, "gw", device);
    ka2 = KeyAttribute.ofAttributeDescriptor(gateway, "gw", device);
    assertEquals(ka, ka2);
    ka = KeyAttribute.ofSingleWildcardAttribute(gateway, "gw", device, "device.1");
    ka2 = KeyAttribute.ofSingleWildcardAttribute(gateway, "gw", device, "device.1");
    assertEquals(ka, ka2);
    try {
      KeyAttribute.ofSingleWildcardAttribute(gateway, "gw", status, status.getName());
      fail("Should have thrown exception");
    } catch (IllegalArgumentException ex) {
      // pass
    }
    try {
      KeyAttribute.ofSingleWildcardAttribute(gateway, "gw", device, status.getName());
      fail("Should have thrown exception");
    } catch (IllegalArgumentException ex) {
      // pass
    }
  }
}
