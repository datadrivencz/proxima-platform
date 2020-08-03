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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InstancesTest {

  /** Mock class. */
  public static class First {

    private final String name;

    public First() {
      name = "first";
    }
  }

  /** Mock class. */
  public static class Second {

    private Second() {
      // No-op.
    }
  }

  public static class Third {

    private final String name;

    public Third(String name) {
      this.name = name;
    }
  }

  public static class Fourth {

    private final String name;
    private final Integer number;

    public Fourth(String name, Integer number) {
      this.name = name;
      this.number = number;
    }
  }

  @Test
  public void testCreate() {
    final First instance = Instances.create(First.class);
    Assertions.assertEquals("first", instance.name);
  }

  /** Make sure we don't allow instantiation using inaccessible constructors. */
  @Test
  public void testCreateWithPrivateConstructor() {
    final IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> Instances.create(Second.class));
    Assertions.assertEquals(
        String.format("Unable to find suitable constructor for class [%s].", Second.class),
        exception.getMessage());
  }

  @Test
  public void testCreateWithSingleArgument() {
    final Third instance =
        Instances.create(Third.class, new Instances.Parameter<>(String.class, "third"));
    Assertions.assertEquals("third", instance.name);
  }

  @Test
  public void testCreateWithMultipleArguments() {
    final IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> Instances.create(Fourth.class));
    Assertions.assertEquals(
        String.format("Unable to find suitable constructor for class [%s].", Fourth.class),
        exception.getMessage());
    final Fourth instance =
        Instances.create(
            Fourth.class,
            new Instances.Parameter<>(String.class, "fourth"),
            new Instances.Parameter<>(Integer.class, 1));
    Assertions.assertEquals("fourth", instance.name);
    Assertions.assertEquals(1, instance.number);
  }

  @Test
  public void testMultipleArgumentsWithTheSameType() {
    final UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                Instances.create(
                    Fourth.class,
                    new Instances.Parameter<>(String.class, "foo"),
                    new Instances.Parameter<>(String.class, "bar")));
    Assertions.assertEquals(
        "Multiple parameters with the same type are not supported.", exception.getMessage());
  }
}
