/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.tools.io;

import cz.o2.proxima.storage.StreamElement;

/**
 * A typed ingest for purpose of console processing.
 */
public class TypedStreamElement<T> extends StreamElement {

  public static <T> TypedStreamElement<T> of(StreamElement element) {
    return new TypedStreamElement<>(element);
  }

  @SuppressWarnings("unchecked")
  private TypedStreamElement(StreamElement element) {
    super(
        element.getEntityDescriptor(), element.getAttributeDescriptor(),
        element.getUuid(), element.getKey(), element.getAttribute(),
        element.getStamp(), false, element.getValue());
  }

  @Override
  public String toString() {
    return "TypedIngest(entityDesc=" + getEntityDescriptor()
        + ", attrDesc=" + getAttributeDescriptor()
        + ", key=" + getKey()
        + ", attribute=" + getAttribute()
        + ", stamp=" + getStamp()
        + ", value=" + getParsed() + ")";
  }


}
