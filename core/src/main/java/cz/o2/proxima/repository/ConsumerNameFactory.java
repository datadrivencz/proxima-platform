/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.repository;

import java.io.Serializable;

/** Consumer name generator. */
public interface ConsumerNameFactory<T> extends Serializable {

  /**
   * Initialize for given context.
   *
   * @param context the context of this generator context
   */
  void setup(T context);

  /**
   * Get consumer name
   *
   * @return String consumer name
   */
  String apply();
}
