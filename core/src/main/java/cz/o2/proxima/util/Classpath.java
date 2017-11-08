/**
 * Copyright 2017 O2 Czech Republic, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Classpath related utilities.
 */
public class Classpath {
  
  private static final Logger LOG = LoggerFactory.getLogger(Classpath.class);
  
  /** Find given class.
   * Try hard to find it replacing `.' by `$' if
   * appropriate.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <T> Class<T> findClass(String name, Class<T> superClass)
      throws ClassNotFoundException {
   
    Class clz;
    if ((clz = instantiateClass(name)) != null) {
      return clz;
    }
    while (true) {
      // try to replace dots by $ in class name from the end until no dots exist
      int lastDot = name.lastIndexOf('.');
      if (lastDot == -1) {
        break;
      }
      String newName = name.substring(0, lastDot) + "$";
      if (lastDot < name.length() - 1) {
        newName += name.substring(lastDot + 1);
      }
      name = newName;
      if ((clz = instantiateClass(name)) != null) {
        return clz;
      }
    }
    throw new ClassNotFoundException("Cannot find class " + name);
  }
    
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static Class instantiateClass(String name) {
    try {
      return Thread.currentThread().getContextClassLoader().loadClass(name);
    } catch (ClassNotFoundException t) {
      LOG.debug("Cannot instantiate class {}", name, t);
      return null;
    }
  }

}
