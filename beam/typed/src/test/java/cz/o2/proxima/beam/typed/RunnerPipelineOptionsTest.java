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

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RunnerPipelineOptionsTest {

  /** Mock materializer. */
  public static class FirstMaterializer implements SingleOutputPipelineMaterializer<String> {

    @Override
    public void registerInputs(RegisterInputsContext ctx) {
      // Noop.
    }

    @Override
    public void registerOutputs(RegisterOutputsContext ctx) {
      // Noop.
    }

    @Override
    public PCollection<TypedElement<String>> materialize(MaterializeContext ctx) {
      return null;
    }
  }

  /** Mock materializer. */
  public static class SecondMaterializer implements SingleOutputPipelineMaterializer<String> {

    @Override
    public void registerInputs(RegisterInputsContext ctx) {
      // Noop.
    }

    @Override
    public void registerOutputs(RegisterOutputsContext ctx) {
      // Noop.
    }

    @Override
    public PCollection<TypedElement<String>> materialize(MaterializeContext ctx) {
      return null;
    }
  }

  @Test
  public void testSingleMaterializer() {
    final RunnerPipelineOptions options =
        PipelineOptionsFactory.fromArgs(
                String.format("--materializers=%s", FirstMaterializer.class.getName()))
            .withValidation()
            .as(RunnerPipelineOptions.class);
    Assertions.assertEquals(1, options.getMaterializers().size());
    Assertions.assertEquals(FirstMaterializer.class, options.getMaterializers().get(0));
  }

  @Test
  public void testMultipleMaterializers() {
    final RunnerPipelineOptions options =
        PipelineOptionsFactory.fromArgs(
                String.format(
                    "--materializers=%s,%s ",
                    FirstMaterializer.class.getName(), SecondMaterializer.class.getName()))
            .withValidation()
            .as(RunnerPipelineOptions.class);
    Assertions.assertEquals(2, options.getMaterializers().size());
    Assertions.assertEquals(FirstMaterializer.class, options.getMaterializers().get(0));
    Assertions.assertEquals(SecondMaterializer.class, options.getMaterializers().get(1));
  }

  @Test
  public void testMissingMaterializers() {
    final IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PipelineOptionsFactory.fromArgs("")
                    .withValidation()
                    .as(RunnerPipelineOptions.class));
    Assertions.assertTrue(exception.getMessage().contains("--materializers"));
  }
}
