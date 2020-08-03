/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.typed;

import com.google.common.collect.Iterables;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.ExceptionUtils;
import java.net.URI;
import java.util.Collections;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TypedElementCoderProviderTest {

  private static AttributeDescriptor<String> createAttributeDescriptor(String name) {
    return AttributeDescriptor.newBuilder(Repository.ofTest(ConfigFactory.empty()))
        .setEntity("entity")
        .setName(name)
        .setSchemeUri(URI.create("string://whatever"))
        .build();
  }

  @Test
  public void testSingleDescriptor() throws CannotProvideCoderException {
    final AttributeDescriptor<String> fooDescriptor = createAttributeDescriptor("foo");
    final TypedElementCoderProvider coderProvider = new TypedElementCoderProvider();
    coderProvider.registerAttribute(fooDescriptor);
    final TypedElementCoder<String> coder =
        (TypedElementCoder<String>)
            coderProvider.coderFor(
                TypedElement.typeDescriptor(fooDescriptor), Collections.emptyList());
    Assertions.assertEquals(
        fooDescriptor, Iterables.getOnlyElement(coder.getAttributeDescriptor()));
  }

  @Test
  public void testMultipleDescriptorsWithSameType() throws CannotProvideCoderException {
    final AttributeDescriptor<String> fooDescriptor = createAttributeDescriptor("foo");
    final AttributeDescriptor<String> barDescriptor = createAttributeDescriptor("bar");
    final TypedElementCoderProvider coderProvider = new TypedElementCoderProvider();
    coderProvider.registerAttribute(fooDescriptor).registerAttribute(barDescriptor);
    final TypedElementCoder<String> fooCoder =
        (TypedElementCoder<String>)
            coderProvider.coderFor(
                TypedElement.typeDescriptor(fooDescriptor), Collections.emptyList());
    final TypedElementCoder<String> barCoder =
        (TypedElementCoder<String>)
            coderProvider.coderFor(
                TypedElement.typeDescriptor(fooDescriptor), Collections.emptyList());
    Assertions.assertEquals(fooCoder, barCoder);
    Assertions.assertEquals(2, fooCoder.getAttributeDescriptor().size());
    Assertions.assertEquals(2, barCoder.getAttributeDescriptor().size());
  }

  @Test
  public void testUnknownTypeDescriptor() {
    Assertions.assertThrows(
        CannotProvideCoderException.class,
        () -> {
          final AttributeDescriptor<String> fooDescriptor = createAttributeDescriptor("foo");
          final TypedElementCoderProvider coderProvider = new TypedElementCoderProvider();
          coderProvider.coderFor(
              TypedElement.typeDescriptor(fooDescriptor), Collections.emptyList());
        });
  }

  @Test
  public void testAllRepositoryRegistered() {
    Repository repository = ConfigRepository.ofTest(ConfigFactory.load("test-reference.conf"));
    TypedElementCoderProvider coderProvider = TypedElementCoderProvider.of(repository);
    repository
        .getAllEntities()
        .flatMap(e -> e.getAllAttributes().stream())
        .forEach(
            attr -> {
              Coder<? extends TypedElement<?>> coder =
                  ExceptionUtils.uncheckedFactory(
                      () ->
                          coderProvider.coderFor(
                              TypedElement.typeDescriptor(attr), Collections.emptyList()));
              Assertions.assertNotNull(coder);
            });
  }
}
