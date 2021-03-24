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
package cz.o2.proxima.direct.transaction;

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.TransactionMode;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.util.Optionals;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Internal
public class TransactionUtils {

  /**
   * Retrieve a writer for transactional requests related to given entities.
   *
   * @param direct {@link DirectDataOperator} to create the writer
   * @param attributes {@link AttributeDescriptor AttributeDescriptors} that will be part of the
   *     transaction (both input and output)
   * @return writer that should be used to write the transaction requests
   */
  @SafeVarargs
  public static OnlineAttributeWriter getRequestWriter(
      DirectDataOperator direct, AttributeDescriptor<?>... attributes) {

    EntityDescriptor transaction = direct.getRepository().getEntity("_transaction");
    AttributeDescriptor<Request> request = transaction.getAttribute("request.*");
    DirectAttributeFamilyDescriptor family =
        findFamilyForTransactionalAttribute(direct, request, attributes);
    return Optionals.get(family.getWriter()).online();
  }

  /**
   * Retrieve {@link CommitLogReader} for reading response to transactional requests.
   *
   * @param direct {@link DirectDataOperator} to create the writer
   * @param attributes {@link AttributeDescriptor AttributeDescriptors} that will be part of the
   *     transaction (both input and output)
   * @return reader that should be used to observer the transaction responses
   */
  public static CommitLogReader getResponseReader(
      DirectDataOperator direct, AttributeDescriptor<?>... attributes) {

    EntityDescriptor transaction = direct.getRepository().getEntity("_transaction");
    AttributeDescriptor<Response> response = transaction.getAttribute("response.*");
    DirectAttributeFamilyDescriptor family =
        findFamilyForTransactionalAttribute(direct, response, attributes);
    return Optionals.get(family.getCommitLogReader());
  }

  private static DirectAttributeFamilyDescriptor findFamilyForTransactionalAttribute(
      DirectDataOperator direct,
      AttributeDescriptor<?> transactionalAttribute,
      AttributeDescriptor<?>... attributes) {

    Preconditions.checkArgument(
        attributes.length > 0, "Cannot return writer for empty attribute list");

    TransactionMode mode = attributes[0].getTransactionMode();
    Preconditions.checkArgument(
        Arrays.stream(attributes).allMatch(a -> a.getTransactionMode() == mode),
        "All passed attributes must have the same transaction mode. Got attributes %s.",
        Arrays.toString(attributes));

    List<DirectAttributeFamilyDescriptor> candidates =
        Arrays.stream(attributes)
            .flatMap(a -> a.getTransactionalManagerFamilies().stream())
            .distinct()
            .map(direct::findFamilyByName)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(af -> af.getDesc().getAttributes().contains(transactionalAttribute))
            .collect(Collectors.toList());
    Preconditions.checkState(
        candidates.size() == 1,
        "Should have received only single family for attribute %s, got %s",
        transactionalAttribute,
        candidates);
    return candidates.get(0);
  }

  private TransactionUtils() {}
}
