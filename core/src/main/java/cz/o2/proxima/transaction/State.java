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
import java.util.Collections;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Internal
@ToString
@EqualsAndHashCode
public class State implements Serializable {

  public static State open(Set<KeyAttribute> inputAttributes) {
    return new State(Flags.OPEN, inputAttributes);
  }

  public static State empty() {
    return new State(Flags.UNKNOWN, Collections.emptySet());
  }

  public enum Flags {
    UNKNOWN,
    OPEN,
    COMMITTED
  }

  @Getter private final Flags flags;
  @Getter private final Set<KeyAttribute> inputAttributes;

  public State() {
    this(Flags.UNKNOWN, Collections.emptySet());
  }

  private State(Flags flags, Set<KeyAttribute> inputAttributes) {
    this.flags = flags;
    this.inputAttributes = inputAttributes;
  }
}
