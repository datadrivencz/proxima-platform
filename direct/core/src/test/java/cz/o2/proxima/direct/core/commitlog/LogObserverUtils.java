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
package cz.o2.proxima.direct.core.commitlog;

import cz.o2.proxima.core.functional.Consumer;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.functional.UnaryPredicate;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OnNextContext;
import java.util.List;

public class LogObserverUtils {

  public static CommitLogObserver toList(List<StreamElement> list, Consumer<Boolean> onFinished) {
    return toList(list, onFinished, ign -> true);
  }

  public static CommitLogObserver toList(
      List<StreamElement> list,
      Consumer<Boolean> onFinished,
      UnaryPredicate<StreamElement> shouldContinue) {

    return toList(list, Pair::getFirst, onFinished, shouldContinue);
  }

  public static <X> CommitLogObserver toList(
      List<X> list,
      UnaryFunction<Pair<StreamElement, OnNextContext>, X> mapFn,
      Consumer<Boolean> onFinished,
      UnaryPredicate<StreamElement> shouldContinue) {

    return new CommitLogObserver() {
      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

      @Override
      public boolean onNext(StreamElement element, OnNextContext context) {
        list.add(mapFn.apply(Pair.of(element, context)));
        context.confirm();
        return shouldContinue.apply(element);
      }

      @Override
      public void onCompleted() {
        onFinished.accept(true);
      }

      @Override
      public void onCancelled() {
        onFinished.accept(false);
      }
    };
  }

  public static <T> CommitLogObserver toList(List<T> list, AttributeDescriptor<T> attribute) {
    return (ingest, context) -> {
      if (ingest.getAttributeDescriptor().equals(attribute)) {
        attribute.valueOf(ingest).ifPresent(list::add);
      }
      context.confirm();
      return true;
    };
  }
}
