/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam;

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Experimental;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.source.UnboundedStreamSource;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.seznam.euphoria.beam.io.BeamUnboundedSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * IO for Apache Beam for reading and writing of named entities.
 */
@Experimental("Need more valid use-cases, euphoria-beam not stable")
public class ProximaIO implements Serializable {

  /**
   * Create new {@link ProximaIO} for given {@link Repository}.
   * @param repo the repository to read
   * @return new {@link ProximaIO}
   */
  public static ProximaIO from(Repository repo) {
    return new ProximaIO(repo);
  }

  private final Repository repo;

  private ProximaIO(Repository repo) {
    this.repo = repo;
  }

  /**
   * Create {@link PCollection} for given attributes.
   * The created {@link PCollection} will represent streaming data
   * read from configured (primary) attribute families.
   * @param pipeline the pipeline to add the {@link PCollection} to.
   * @param position position to read the associated commit-log from
   * @param attrs list of attributes to read
   * @return {@link PCollection} representing the attributes
   */
  public PCollection<StreamElement> read(
      Pipeline pipeline,
      Position position,
      AttributeDescriptor<?>... attrs) {

    Preconditions.checkArgument(
        attrs.length > 0,
        "Please provide non-empty list of attributes");

    PCollectionList<StreamElement> list = PCollectionList.empty(pipeline);
    Stream.of(attrs)
        .map(a -> repo.getFamiliesForAttribute(a)
            .stream()
            .filter(af -> af.getType() == StorageType.PRIMARY)
            .filter(af -> af.getAccess().canReadCommitLog())
            .findAny()
            .orElseThrow(() -> new IllegalArgumentException(
                "Attribute " + a + " has no primary commit-log")))
        .collect(Collectors.toSet())
        .stream()
        .map(af -> af.getCommitLogReader().get())
        .map(reader -> UnboundedStreamSource.of(reader, position))
        .map(BeamUnboundedSource::wrap)
        // FIXME: port this to euphoria-beam full blown API, including
        // coder registration
        .map(s -> pipeline.apply(Read.from(s)).setCoder(StreamElementCoder.of(repo)))
        .forEach(list::and);

    return list.apply(Flatten.pCollections()).setCoder(StreamElementCoder.of(repo));
  }

  /**
   * Persist given {@link PCollection} of {@link StreamElement} according
   * to given {@link Repository}.
   * @param collection the {@link PCollection} to persist
   */
  public void write(PCollection<StreamElement> collection) {
    collection.apply(ParDo.of(new DoFn<StreamElement, Void>() {

      final Set<String> unconfirmed = Collections.synchronizedSet(new HashSet<>());

      @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
      @StartBundle
      public void startBundle() {
        unconfirmed.clear();
      }

      @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
      @ProcessElement
      public void process(ProcessContext context) {
        StreamElement element = context.element();
        final String uuid = element.getUuid();
        OnlineAttributeWriter writer = repo
            .getWriter(element.getAttributeDescriptor())
            .orElseThrow(() -> new IllegalArgumentException(
                "Missing writer for " + element.getAttribute()));
        unconfirmed.add(uuid);
        writer.write(element, (succ, exc) -> {
          if (!succ) {
            throw new RuntimeException(exc);
          }
          unconfirmed.remove(uuid);
          synchronized (unconfirmed) {
            unconfirmed.notify();
          }
        });
      }

      @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
      @FinishBundle
      public void finishBundle() throws InterruptedException {
        while (!unconfirmed.isEmpty()) {
          synchronized (unconfirmed) {
            unconfirmed.wait(1000);
          }
        }
      }

    }));
  }

}
