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
package cz.o2.proxima.transform;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class CopyIndexTransformTest {

  private final Repository repo = Repository.of(ConfigFactory.load("test-transactions.conf"));
  private final EntityDescriptor user = repo.getEntity("user");
  private final Wildcard<byte[]> userGateways = Wildcard.of(user, user.getAttribute("gateway.*"));
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final Wildcard<byte[]> gatewayUsers =
      Wildcard.of(gateway, gateway.getAttribute("user.*"));
  private final Transformation transform =
      repo.getTransformations().get("user-gateway-1").getTransformation();
  private final long now = System.currentTimeMillis();

  @Test
  public void testTransform() {
    List<StreamElement> results = new ArrayList<>();
    assertEquals(
        1,
        transform
            .asElementWiseTransform()
            .apply(gatewayUsers.upsert("gateway", "user", now, new byte[] {1, 2}), results::add));
    assertEquals(1, results.size());
    checkEqualsUpToUuid(
        userGateways.upsert("user", "gateway", now, new byte[] {1, 2}), results.get(0));

    results.clear();
    assertEquals(
        1,
        transform
            .asElementWiseTransform()
            .apply(
                gatewayUsers.upsert(1L, "gateway", "user", now, new byte[] {1, 2}), results::add));
    assertEquals(1, results.size());
    checkEqualsUpToUuid(
        userGateways.upsert(1L, "user", "gateway", now, new byte[] {1, 2}), results.get(0));

    results.clear();
    assertEquals(
        1,
        transform
            .asElementWiseTransform()
            .apply(gatewayUsers.delete("gateway", "user", now), results::add));
    assertEquals(1, results.size());
    checkEqualsUpToUuid(userGateways.delete("user", "gateway", now), results.get(0));

    results.clear();
    assertEquals(
        1,
        transform
            .asElementWiseTransform()
            .apply(gatewayUsers.delete(1L, "gateway", "user", now), results::add));
    assertEquals(1, results.size());
    checkEqualsUpToUuid(userGateways.delete(1L, "user", "gateway", now), results.get(0));
  }

  private void checkEqualsUpToUuid(StreamElement expected, StreamElement actual) {
    assertEquals(expected.getKey(), actual.getKey());
    assertEquals(expected.getAttribute(), actual.getAttribute());
    assertEquals(expected.getAttributeDescriptor(), actual.getAttributeDescriptor());
    assertEquals(expected.getEntityDescriptor(), actual.getEntityDescriptor());
    assertEquals(expected.getStamp(), actual.getStamp());
    assertEquals(expected.hasSequentialId(), actual.hasSequentialId());
    if (expected.hasSequentialId()) {
      assertEquals(expected.getSequentialId(), actual.getSequentialId());
    }
    assertEquals(expected.isDelete(), actual.isDelete());
    assertEquals(expected.isDeleteWildcard(), actual.isDeleteWildcard());
    assertArrayEquals(expected.getValue(), actual.getValue());
  }
}
