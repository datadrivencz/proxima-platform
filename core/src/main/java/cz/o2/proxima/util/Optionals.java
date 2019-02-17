/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.util;

import java.util.Optional;

/**
 * Utility class for manipulation with {@link Optional}
 */
public class Optionals {
  /**
   * Get value from Optional or throw IllegalArgumentException
   *
   * @param optional Optional object
   * @param <T>      Generic type Optional
   * @return T value from Optional
   * @throws IllegalArgumentException in case of empty value
   */
  public static <T> T get(Optional<T> optional) {
    return optional.orElseThrow(() ->
        new IllegalArgumentException("Provided optional is empty."));
  }

  private Optionals() {
    // nop
  }

}
