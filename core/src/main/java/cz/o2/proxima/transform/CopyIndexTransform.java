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
package cz.o2.proxima.transform;

import com.google.common.base.Preconditions;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CopyIndexTransform implements ElementWiseTransformation {

  private final List<Pair<EntityDescriptor, AttributeDescriptor<?>>> copies;

  public CopyIndexTransform(List<Pair<EntityDescriptor, AttributeDescriptor<?>>> attributes) {
    this.copies = attributes;
    Preconditions.checkArgument(
        attributes.stream().map(Pair::getSecond).allMatch(AttributeDescriptor::isWildcard),
        "Copy index works on wildcard attributes only. Got [ %s ]",
        attributes);
    Preconditions.checkArgument(
        attributes.stream().map(a -> a.getSecond().getSchemeUri()).distinct().count() == 1,
        "All attributes must have the same scheme. Got attributes [ %s ]",
        attributes);
  }

  @Override
  public void setup(Repository repo, Map<String, Object> cfg) {}

  @Override
  public int apply(StreamElement input, Collector<StreamElement> collector) {
    String key = input.getKey();
    String suffix =
        input.getAttribute().substring(input.getAttributeDescriptor().toAttributePrefix().length());
    Preconditions.checkArgument(
        !input.isDeleteWildcard(), "Wildcard deletes not supported. Got [ %s ]", input);
    for (Pair<EntityDescriptor, AttributeDescriptor<?>> target : copies) {
      if (!target.getSecond().equals(input.getAttributeDescriptor())) {
        if (input.isDelete()) {
          if (input.hasSequentialId()) {
            collector.collect(
                StreamElement.delete(
                    target.getFirst(),
                    target.getSecond(),
                    input.getSequentialId(),
                    suffix,
                    target.getSecond().toAttributePrefix() + key,
                    input.getStamp()));
          } else {
            collector.collect(
                StreamElement.delete(
                    target.getFirst(),
                    target.getSecond(),
                    UUID.randomUUID().toString(),
                    suffix,
                    target.getSecond().toAttributePrefix() + key,
                    input.getStamp()));
          }
        } else {
          if (input.hasSequentialId()) {
            collector.collect(
                StreamElement.upsert(
                    target.getFirst(),
                    target.getSecond(),
                    input.getSequentialId(),
                    suffix,
                    target.getSecond().toAttributePrefix() + key,
                    input.getStamp(),
                    input.getValue()));
          } else {
            collector.collect(
                StreamElement.upsert(
                    target.getFirst(),
                    target.getSecond(),
                    UUID.randomUUID().toString(),
                    suffix,
                    target.getSecond().toAttributePrefix() + key,
                    input.getStamp(),
                    input.getValue()));
          }
        }
      }
    }
    return copies.size() - 1;
  }
}
