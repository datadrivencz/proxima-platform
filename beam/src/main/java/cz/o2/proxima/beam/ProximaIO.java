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
import cz.o2.proxima.beam.io.CommitLogSource;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.BulkAttributeWriter;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;

/**
 * IO for Apache Beam for reading and writing of named entities.
 */
@Experimental("Need more valid use-cases, euphoria-beam not stable")
@Slf4j
public class ProximaIO implements Serializable {

  /**
   * Create new {@link ProximaIO} for given {@link Repository}.
   * @param repo the repository to read
   * @return new {@link ProximaIO}
   */
  public static ProximaIO from(Repository repo) {
    return new ProximaIO(repo, null, 0);
  }

  @Getter
  private final Repository repo;

  @Nullable
  private final Duration maxReadTime;

  private final long allowedLateness;

  private ProximaIO(Repository repo, Duration maxReadTime, long allowedLateness) {
    this.repo = Objects.requireNonNull(repo);
    this.maxReadTime = maxReadTime;
    this.allowedLateness = allowedLateness;
  }

  /**
   * Specify maximal reading time from created sources.
   * @param maxReadTime the maximal reading time after which the reading is
   *                    terminated
   * @return new {@link ProximaIO} with the given maxReadTime
   */
  public ProximaIO withMaxReadTime(Duration maxReadTime) {
    return new ProximaIO(repo, maxReadTime, allowedLateness);
  }

  /**
   * Specify allowed lateness for generated sources.
   * @param allowedLateness the allowed lateness in milliseconds
   * @return new {@link ProximaIO} with the given allowedLateness
   */
  public ProximaIO withAllowedLateness(long allowedLateness) {
    return new ProximaIO(repo, maxReadTime, allowedLateness);
  }

  /**
   * Create {@link PCollection} for given attributes.
   * The created {@link PCollection} will represent streaming data
   * read from configured (primary) attribute families.
   * @param position position to read the associated commit-log from
   * @param attrs list of attributes to read
   * @return {@link PCollection} representing the attributes
   */
  public PTransform<PBegin, PCollection<StreamElement>> read(
      Position position,
      AttributeDescriptor<?>... attrs) {

    return read(position, Arrays.asList(attrs));
  }

  /**
   * Create {@link PCollection} for given attributes.
   * The created {@link PCollection} will represent streaming data
   * read from configured (primary) attribute families.
   * @param position position to read the associated commit-log from
   * @param attrs list of attributes to read
   * @return {@link PCollection} representing the attributes
   */
  public PTransform<PBegin, PCollection<StreamElement>> read(
      Position position,
      List<AttributeDescriptor<?>> attrs) {

    Preconditions.checkArgument(
        !attrs.isEmpty(),
        "Please provide non-empty list of attributes");

    return new PTransform<PBegin, PCollection<StreamElement>>() {

      @Override
      public PCollection<StreamElement> expand(PBegin input) {
        Pipeline pipeline = input.getPipeline();
        PCollectionList<StreamElement> list = PCollectionList.empty(pipeline);
        attrs.stream()
            .map(a -> repo.getFamiliesForAttribute(a)
                .stream()
                .filter(af -> af.getType() == StorageType.PRIMARY)
                .filter(af -> af.getAccess().canReadCommitLog())
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException(
                    "Attribute " + a + " has no primary commit-log")))
            .collect(Collectors.toSet())
            .stream()
            .map(af -> {
              log.debug("Reading attribute family {}", af);
              return af.getCommitLogReader().get();
            })
            .map(reader -> CommitLogSource.of(repo, reader, position, allowedLateness))
            .map(s -> pipeline
                .apply(withReadTime(Read.from(s))).setCoder(StreamElementCoder.of(repo))
                .apply(MapElements.via(new SimpleFunction<StreamElement, StreamElement>(e -> {
                  System.err.println(" *** AFTER MAP " + e);
                  return e;
                }) { })))
            .forEach(list::and);
        return list
            .apply(Flatten.pCollections())
            .setCoder(StreamElementCoder.of(repo));
      }

    };
  }

  /**
   * Read all attributes from given {@link AttributeFamilyDescriptor}.
   * @param family the family to read from
   * @param position the position to read from
   * @return {@link PTransform} to apply to create {@link PCollection}
   * representing the family
   */
  public PTransform<PBegin, PCollection<StreamElement>> streamFrom(
      AttributeFamilyDescriptor family,
      Position position) {

    CommitLogSource source = CommitLogSource.of(
        repo, family.getCommitLogReader()
            .orElseThrow(() -> new IllegalArgumentException(
                "Family " + family + " has no commit log")),
        position, allowedLateness);

    return new PTransform<PBegin, PCollection<StreamElement>>() {

      @Override
      public PCollection<StreamElement> expand(PBegin input) {
        Pipeline pipeline = input.getPipeline();
        return pipeline.apply(withReadTime(Read.from(source)))
            .setCoder(StreamElementCoder.of(repo));
      }

    };

  }

  /**
   * Persist given {@link PCollection} of {@link StreamElement} according
   * to given {@link Repository}.
   * This operation requires all attributes present in the {@link PCollection}
   * to have {@link OnlineAttributeWriter}, otherwise
   * {@link ClassCastException} will be thrown. In that case use {@link ProximaIO#writeBulk}.
   * @return persisting {@link PTransform}
   */
  public PTransform<PCollection<StreamElement>, PDone> write() {

    return write(
        attr -> repo
                .getWriter(attr)
                .orElseThrow(() -> new IllegalArgumentException(
                    "Missing writer for " + attr)));
  }

  /**
   * Persist given {@link PCollection} using either {@link OnlineAttributeWriter}
   * or {@link BulkAttributeWriter}.
   * @param windowLength length of bulk window
   * @param unit timeunit of the window length
   * @param parallelism the parallelism at which to create writers
   * @return {@link PTransform} for persisting the {@link PCollection}
   */
  public PTransform<PCollection<StreamElement>, PDone> writeBulk(
      int windowLength, TimeUnit unit, int parallelism) {

    return writeBulk(
        windowLength, unit, parallelism,
        attr -> repo.getFamiliesForAttribute(attr)
            .stream()
            .filter(af -> af.getType() == StorageType.PRIMARY)
            .filter(af -> !af.getAccess().isReadonly())
            .findFirst()
            .flatMap(AttributeFamilyDescriptor::getWriter)
            .map(w -> w.bulk())
            .orElseThrow(() -> new IllegalArgumentException(
                "Attribute " + attr + " has no writable primary storage")));
  }

  PTransform<PCollection<StreamElement>, PDone> write(
      UnaryFunction<AttributeDescriptor, OnlineAttributeWriter> writerFn) {

    return new PTransform<PCollection<StreamElement>, PDone>() {
      @Override
      public PDone expand(PCollection<StreamElement> input) {
        Pipeline pipeline = input.getPipeline();
        input.apply(ParDo.of(new DoFn<StreamElement, Void>() {

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
            OnlineAttributeWriter writer = writerFn.apply(
                element.getAttributeDescriptor());
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

        return PDone.in(pipeline);
      }

    };
  }


  PTransform<PCollection<StreamElement>, PDone> writeBulk(
      long windowLength, TimeUnit unit, int parallelism,
      UnaryFunction<AttributeDescriptor, BulkAttributeWriter> writerFn) {

    Preconditions.checkArgument(windowLength > 0, "Window must have positive length");
    Preconditions.checkArgument(parallelism > 0, "Parallelism must be positive");
    return new PTransform<PCollection<StreamElement>, PDone>() {
      @Override
      public PDone expand(PCollection<StreamElement> input) {
        input
            .apply(Window.into(FixedWindows.of(org.joda.time.Duration.millis(unit.toMillis(windowLength)))))
            .apply(MapElements.via(
                new SimpleFunction<StreamElement, KV<Integer, StreamElement>>(
                    e -> KV.of(e.hashCode() % parallelism, e)) { }))
            .apply(GroupByKey.create())
            .apply(ParDo.of(new DoFn<KV<Integer, Iterable<StreamElement>>, Void>() {

                @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
                @ProcessElement
                public void process(ProcessContext context) throws InterruptedException {
                  Set<String> unconfirmed = Collections.synchronizedSet(new HashSet<>());
                  Map<AttributeDescriptor, AttributeWriterBase> writers = new HashMap<>();
                  for (StreamElement element : context.element().getValue()) {
                    System.err.println(" *** writeBulk " + element);
                    final String uuid = element.getUuid();
                    AttributeWriterBase writer = writers.computeIfAbsent(
                        element.getAttributeDescriptor(),
                        writerFn::apply);
                    if (writer.getType() == AttributeWriterBase.Type.ONLINE) {
                      writer.online().write(element, (succ, exc) -> {
                        if (!succ) {
                          throw new RuntimeException(exc);
                        }
                        unconfirmed.remove(uuid);
                        synchronized (unconfirmed) {
                          unconfirmed.notify();
                        }
                      });
                    } else {
                      writer.bulk().write(element, (succ, exc) -> {
                        if (!succ) {
                          throw new RuntimeException(exc);
                        }
                      });
                    }
                  }
                  // wait until all processed
                  writers.values().forEach(w -> {
                    if (w.getType() == AttributeWriterBase.Type.BULK) {
                      w.bulk().flush();
                    }
                  });
                  while (!unconfirmed.isEmpty()) {
                    synchronized (unconfirmed) {
                      unconfirmed.wait(1000);
                    }
                  }
                }

            }));

        return PDone.in(input.getPipeline());
      }

    };
  }

  private <T> PTransform<PBegin, PCollection<T>> withReadTime(
      Read.Unbounded<T> what) {

    System.err.println(" **** " + maxReadTime);
    if (maxReadTime != null) {
      log.debug(
          "Applying maxReadTime {} millis to Read.Unbounded",
          maxReadTime.toMillis());
      return what.withMaxReadTime(org.joda.time.Duration.millis(maxReadTime.toMillis()));
    }
    return what;
  }
}
