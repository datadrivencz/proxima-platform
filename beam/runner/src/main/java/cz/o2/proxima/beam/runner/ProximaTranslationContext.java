/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.runner;

import com.google.common.base.Preconditions;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.beam.runner.proto.Messages;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;

public class ProximaTranslationContext {

  private final JobInfo jobInfo;
  private final ProximaPipelineOptions options;
  private final Repository repo;
  private final DirectDataOperator direct;
  private final AttributeFamilyDescriptor shuffleFamily;
  private final AttributeFamilyDescriptor commitFamily;
  private final AttributeFamilyDescriptor stateFamily;
  private final Regular<Messages.Bundle> bundleDesc;
  private final Regular<Messages.Commit> commitDesc;
  private final Wildcard<Messages.State> stateDesc;

  private State state = State.UNKNOWN;

  public ProximaTranslationContext(JobInfo jobInfo, PipelineOptions options) {
    this.jobInfo = jobInfo;
    this.options = options.as(ProximaPipelineOptions.class);
    this.repo = Repository.of(ConfigFactory.load().resolve());
    this.direct = repo.getOrCreateOperator(DirectDataOperator.class);

    this.shuffleFamily = repo.getFamilyByName(this.options.getShuffleFamily());
    this.commitFamily = repo.getFamilyByName(this.options.getCommitFamily());
    this.stateFamily = repo.getFamilyByName(this.options.getStateFamily());

    this.bundleDesc = regular(repo, "bundle", "shuffle");
    this.commitDesc = regular(repo, "bundle", "commit");
    this.stateDesc = wildcard(repo, "state", "data.*");

    validateFamilyHasAttribute(shuffleFamily, bundleDesc);
    validateFamilyHasAttribute(commitFamily, commitDesc);
    validateFamilyHasAttribute(stateFamily, stateDesc);
  }

  private void validateFamilyHasAttribute(
      AttributeFamilyDescriptor familyDesc, AttributeDescriptor<?> attrDesc) {
    Preconditions.checkArgument(
        familyDesc.getAttributes().equals(Collections.singletonList(attrDesc)),
        "Family %s must contain only single attribute %s",
        familyDesc,
        attrDesc);
  }

  private <T> Wildcard<T> wildcard(Repository repo, String entity, String attribute) {
    EntityDescriptor entityDesc = repo.getEntity(entity);
    return Wildcard.of(entityDesc, entityDesc.getAttribute(attribute));
  }

  private <T> Regular<T> regular(Repository repo, String entity, String attribute) {
    EntityDescriptor entityDesc = repo.getEntity(entity);
    return Regular.of(entityDesc, entityDesc.getAttribute(attribute));
  }

  public ExecutorService getExecutor() {
    return direct.getContext().getExecutorService();
  }

  public State getState() {
    return state;
  }

  public State cancel() {
    this.state = State.CANCELLED;
    return getState();
  }

  public State waitUntilFinish(Duration duration) {
    // FIXME
    this.state = State.DONE;
    return getState();
  }

  public MetricResults getMetricsResult() {
    return new MetricResults() {
      @Override
      public @NonNull MetricQueryResults queryMetrics(@NonNull MetricsFilter filter) {
        return null;
      }
    };
  }
}
