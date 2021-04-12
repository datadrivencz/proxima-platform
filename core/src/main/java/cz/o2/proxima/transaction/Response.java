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
import java.io.Serializable;
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
    return new Response(Flags.NONE);
  }

  /**
   * Create response for open transaction.
   *
   * @return response for open transaction
   */
  public static Response open() {
    return new Response(Flags.OPEN);
  }

  public enum Flags {
    NONE,
    OPEN,
    COMMITTED
  }

  @Getter private final Flags flags;

  public Response() {
    this(Flags.NONE);
  }

  private Response(Flags flags) {
    this.flags = flags;
  }
}
