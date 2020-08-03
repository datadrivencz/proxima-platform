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

import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.beam.typed.io.ProximaSQL;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor;
import cz.o2.proxima.storage.commitlog.Position;
import java.time.Instant;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ModelExtension.class)
public class TypedElementSchemaProviderTest {

  @Test
  public void test(ModelContext ctx) throws InterruptedException {
    final Pipeline pipeline = Pipeline.create();

    // Register coders.
    pipeline
        .getCoderRegistry()
        .registerCoderProvider(TypedElementCoderProvider.of(ctx.getModel().getRepo()));

    // Register schemas.
    final TypedElementSchemaProvider schemaProvider =
        new TypedElementSchemaProvider(ctx.getModel().getRepo());
    schemaProvider
        .getDescriptors()
        .forEach(desc -> pipeline.getSchemaRegistry().registerSchemaProvider(desc, schemaProvider));

    final BeamDataOperator operator =
        ctx.getModel().getRepo().getOrCreateOperator(BeamDataOperator.class);

    final EntityAwareAttributeDescriptor.Regular<User> descriptor =
        ctx.getModel().getIdentifier().getUserDescriptor();

    ctx.writeElements(
        descriptor.upsert("key-first", Instant.now(), User.newBuilder().setId("id-first").build()),
        descriptor.upsert(
            "key-second", Instant.now(), User.newBuilder().setId("id-second").build()),
        descriptor.upsert("key-third", Instant.now(), User.newBuilder().setId("id-third").build()));
    final PCollection<Row> result =
        operator
            .getStream(pipeline, Position.OLDEST, true, true, descriptor)
            .apply(TypedElements.of(descriptor))
            .apply(Select.flattenedSchema())
            .apply(
                SqlTransform.query(
                    "SELECT key, value_id FROM PCOLLECTION WHERE key = 'key-second'"));

    pipeline.apply(
        ProximaSQL.of(repository)
            .query("SELECT key, value_id FROM identifier_user WHERE key = 'key-second'"));

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(result.getSchema())
                .withFieldValue("key", "key-second")
                .withFieldValue("value_id", "id-second")
                .build());

    pipeline.run().waitUntilFinish();
  }
}
