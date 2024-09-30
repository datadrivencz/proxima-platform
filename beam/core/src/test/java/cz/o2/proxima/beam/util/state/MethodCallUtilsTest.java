/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.util.state;

import cz.o2.proxima.core.functional.BiConsumer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.StateBinder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.Sum;
import org.junit.Test;

public class MethodCallUtilsTest {

  @Test
  public void testBinders() {
    AtomicReference<BiFunction<Object, byte[], Iterable<StateValue>>> tmp = new AtomicReference<>();
    AtomicReference<BiConsumer<Object, StateValue>> tmp2 = new AtomicReference<>();
    testBinder(MethodCallUtils.createStateReaderBinder(tmp));
    testBinder(MethodCallUtils.createUpdaterBinder(tmp2));
  }

  private void testBinder(StateBinder binder) {
    List<StateSpec<?>> specs =
        Arrays.asList(
            StateSpecs.bag(),
            StateSpecs.value(),
            StateSpecs.map(),
            StateSpecs.multimap(),
            StateSpecs.combining(Sum.ofIntegers()),
            StateSpecs.orderedList(VarIntCoder.of()));
    specs.forEach(s -> testBinder(s, binder));
  }

  private void testBinder(StateSpec<?> s, StateBinder binder) {
    s.bind("dummy", binder);
  }
}
