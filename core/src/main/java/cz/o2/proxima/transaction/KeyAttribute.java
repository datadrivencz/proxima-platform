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

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Experimental;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import java.io.Serializable;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * A combination of key of an entity, attribute descriptor and (optional) specific attribute. The
 * specific attribute is needed when this object needs to describe a specific attribute of wildcard
 * attribute descriptor.
 */
@Experimental
@ToString
@EqualsAndHashCode
public class KeyAttribute implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Create {@link KeyAttribute} for given entity, key and attribute descriptor. This describes
   * either all wildcard attributes of that key or a single regular attribute.
   *
   * @param entity the entity descriptor
   * @param key the entity key
   * @param attributeDescriptor descriptor of wildcard or regular attribute
   */
  public static KeyAttribute ofAttributeDescriptor(
      EntityDescriptor entity, String key, AttributeDescriptor<?> attributeDescriptor) {

    return new KeyAttribute(entity, key, attributeDescriptor, null);
  }

  /**
   * Create {@link KeyAttribute} for given entity, key and specific wildcard attribute. Do not use
   * this for regular attributes, use {@link #ofAttributeDescriptor(EntityDescriptor, String,
   * AttributeDescriptor)} instead.
   *
   * @param entity the entity descriptor
   * @param key the entity key
   * @param attributeDescriptor descriptor of wildcard attribute
   * @param attribute the specific wildcard attribute
   */
  public static KeyAttribute ofSingleWildcardAttribute(
      EntityDescriptor entity,
      String key,
      AttributeDescriptor<?> attributeDescriptor,
      String attribute) {

    Preconditions.checkArgument(attributeDescriptor.isWildcard());
    Preconditions.checkArgument(attribute.startsWith(attributeDescriptor.toAttributePrefix()));
    return new KeyAttribute(entity, key, attributeDescriptor, attribute);
  }

  @Getter private final EntityDescriptor entity;
  @Getter private final String key;
  @Getter private final AttributeDescriptor<?> attributeDescriptor;
  @Nullable private final String attribute;

  private KeyAttribute(
      EntityDescriptor entity,
      String key,
      AttributeDescriptor<?> attributeDescriptor,
      @Nullable String attribute) {

    this.entity = entity;
    this.key = key;
    this.attributeDescriptor = attributeDescriptor;
    this.attribute = attribute;
  }

  public Optional<String> getAttribute() {
    return Optional.ofNullable(attribute);
  }
}
