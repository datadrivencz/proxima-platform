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
import cz.o2.proxima.repository.AttributeDescriptor;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** A transactional request sent to coordinator */
@Internal
@Builder
@ToString
@EqualsAndHashCode
public class Request implements Serializable {

  public enum Flags {}

  @Getter private final List<AttributeDescriptor<?>> inputAttributes;
  @Getter private final List<AttributeDescriptor<?>> outputAttributes;
  @Getter private final Flags flags;

  public Request() {
    this(Collections.emptyList(), Collections.emptyList(), null);
  }

  public Request(
      List<AttributeDescriptor<?>> inputAttributes,
      List<AttributeDescriptor<?>> outputAttributes,
      Flags flags) {

    this.inputAttributes = inputAttributes;
    this.outputAttributes = outputAttributes;
    this.flags = flags;
  }
}
