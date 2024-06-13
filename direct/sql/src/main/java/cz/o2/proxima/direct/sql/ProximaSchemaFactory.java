/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.sql;

import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.typesafe.config.Config;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

public class ProximaSchemaFactory implements SchemaFactory {

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    Object uri = operand.get("modelUri");
    Config config =
        Optional.ofNullable(operand.get("config"))
            .map(Object::toString)
            .map(n -> ConfigFactory.load(n).resolve())
            .orElseGet(() -> ConfigFactory.load().resolve());
    return new RepositorySchema(Repository.of(config));
  }
}
