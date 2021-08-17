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

import com.google.auto.service.AutoService;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transform.ElementWiseTransformation;
import cz.o2.proxima.transform.Transformation;
import java.util.Map;

@AutoService(TransactionTransformProvider.class)
public class NoopTransactionTransformProvider implements TransactionTransformProvider {

  @Override
  public Transformation create() {
    return new ElementWiseTransformation() {
      @Override
      public void setup(Repository repo, Map<String, Object> cfg) {}

      @Override
      public int apply(StreamElement input, Collector<StreamElement> collector) {
        return 0;
      }
    };
  }

  @Override
  public boolean isTest() {
    return true;
  }
}