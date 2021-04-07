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

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Internal
@EqualsAndHashCode
@ToString
@Builder
public class Response implements Serializable {

  /**
   * Create empty {@link Response}.
   *
   * @return empty response
   */
  public static Response empty() {
    return new Response(Flags.NONE, Collections.emptyList());
  }

  /**
   * Create response for open transaction.
   *
   * @return response for open transaction
   */
  public static Response open() {
    return open(Collections.emptyList());
  }

  /**
   * Create response for open transaction with given inputs.
   *
   * @param inputs the input data requested by the transaction
   * @return response for open transaction with given inputs.
   */
  public static Response open(List<StreamElement> inputs) {
    return new Response(Flags.OPEN, inputs);
  }

  public enum Flags {
    NONE,
    OPEN,
    COMMITTED
  }

  @Getter private final Flags flags;
  @Getter private final List<StreamElement> inputData;

  public Response() {
    this(Flags.NONE, Collections.emptyList());
  }

  private Response(Flags flags, List<StreamElement> inputData) {
    this.flags = flags;
    this.inputData = inputData;
  }
}
