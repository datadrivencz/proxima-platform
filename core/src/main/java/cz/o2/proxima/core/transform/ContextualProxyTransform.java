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
package cz.o2.proxima.core.transform;

import cz.o2.proxima.core.annotations.Evolving;
import cz.o2.proxima.core.repository.DataOperator;
import cz.o2.proxima.core.repository.EntityDescriptor;

/** A {@link ProxyTransform} having (operator specific) context. */
@Evolving
public interface ContextualProxyTransform<OP extends DataOperator>
    extends ProxyTransform, DataOperatorAware {

  /**
   * Setup this transform for given entity.
   *
   * @param op Operator
   * @param entity the target attribute descriptor
   */
  default void setup(EntityDescriptor entity, OP op) {
    // nop
  }
}
