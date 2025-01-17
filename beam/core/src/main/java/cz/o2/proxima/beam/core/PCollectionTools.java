/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.core;

import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.internal.com.google.common.collect.Streams;
import java.util.Comparator;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.PerKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/** Various tools related to manipulation with {@link PCollection}s. */
public class PCollectionTools {

  /**
   * Reduce given {@link PCollection} from updates to snapshot.
   *
   * @param name name of the operation
   * @param input the other {@link PCollection} containing updates
   * @return snapshot
   */
  public static PCollection<StreamElement> reduceAsSnapshot(
      @Nullable String name, PCollection<StreamElement> input) {

    PCollection<KV<String, StreamElement>> withKeys =
        input.apply(
            WithKeys.<String, StreamElement>of(
                    e ->
                        e.getAttributeDescriptor().getEntity()
                            + "@"
                            + e.getKey()
                            + "#"
                            + e.getAttribute())
                .withKeyType(TypeDescriptors.strings()));
    PerKey<String, StreamElement, StreamElement> transform =
        Combine.perKey(
            values ->
                Optionals.get(
                    Streams.stream(values).max(Comparator.comparingLong(StreamElement::getStamp))));

    PCollection<KV<String, StreamElement>> combined =
        name == null ? withKeys.apply(transform) : withKeys.apply(name, transform);
    return combined.apply(
        MapElements.into(TypeDescriptor.of(StreamElement.class)).via(KV::getValue));
  }

  private PCollectionTools() {}
}
