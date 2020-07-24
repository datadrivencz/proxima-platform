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
package cz.o2.proxima.direct.core;

import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;

/** Attribute descriptor with associated accessors. */
public class DirectAttributeFamilyDescriptor implements Serializable {

  private static final long serialVersionUID = 1L;

  @Getter private final AttributeFamilyDescriptor desc;

  private final @Nullable AttributeWriterBase.Factory<?> writerFactory;
  private final @Nullable CommitLogReader.Factory commitLogReaderFactory;
  private final @Nullable BatchLogObservable.Factory batchObservableFactory;
  private final @Nullable RandomAccessReader.Factory randomAccessReaderFactory;
  private final @Nullable CachedView.Factory cachedViewFactory;

  @Nullable private transient AttributeWriterBase writer;
  @Nullable private transient CommitLogReader commitLogReader;
  @Nullable private transient BatchLogObservable batchObservable;
  @Nullable private transient RandomAccessReader randomAccessReader;
  @Nullable private transient CachedView cachedView;

  DirectAttributeFamilyDescriptor(
      Repository repository,
      AttributeFamilyDescriptor desc,
      Optional<AttributeWriterBase> writer,
      Optional<CommitLogReader> commitLogReader,
      Optional<BatchLogObservable> batchLogObservable,
      Optional<RandomAccessReader> randomAccessReader,
      Optional<CachedView> cachedView) {

    RepositoryFactory repositoryFactory = repository.asFactory();
    this.desc = desc;
    this.writerFactory = writer.map(w -> w.asFactory(repositoryFactory)).orElse(null);
    this.commitLogReaderFactory =
        commitLogReader.map(r -> r.asFactory(repositoryFactory)).orElse(null);
    this.batchObservableFactory =
        batchLogObservable.map(r -> r.asFactory(repositoryFactory)).orElse(null);
    this.randomAccessReaderFactory =
        randomAccessReader.map(r -> r.asFactory(repositoryFactory)).orElse(null);
    this.cachedViewFactory = cachedView.map(v -> v.asFactory(repositoryFactory)).orElse(null);
  }

  DirectAttributeFamilyDescriptor(
      Repository repo, AttributeFamilyDescriptor desc, Context context, DataAccessor accessor) {

    this(
        repo,
        desc,
        accessor.getWriter(context),
        accessor.getCommitLogReader(context),
        accessor.getBatchLogObservable(context),
        accessor.getRandomAccessReader(context),
        accessor.getCachedView(context));
  }

  public List<AttributeDescriptor<?>> getAttributes() {
    return desc.getAttributes();
  }

  @Override
  public String toString() {
    return desc.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DirectAttributeFamilyDescriptor) {
      DirectAttributeFamilyDescriptor other = (DirectAttributeFamilyDescriptor) obj;
      return other.desc.equals(desc);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return desc.hashCode();
  }

  /**
   * Retrieve writer for this family. Empty if this family is not writable.
   *
   * @return optional {@link AttributeWriterBase} of this family
   */
  public Optional<AttributeWriterBase> getWriter() {
    if (!desc.getAccess().isReadonly()) {
      return Optional.of(
          Objects.requireNonNull(writer(), () -> "Family " + desc.getName() + " has no writer"));
    }
    return Optional.empty();
  }

  private AttributeWriterBase writer() {
    if (writer == null) {
      writer = writerFactory.create();
    }
    return writer;
  }

  /**
   * Retrieve a commit log reader of this family. Empty if this attribute family is not a commit
   * log.
   *
   * @return optional {@link CommitLogReader} of this family
   */
  public Optional<CommitLogReader> getCommitLogReader() {
    if (desc.getAccess().canReadCommitLog()) {
      return Optional.of(
          Objects.requireNonNull(
              commitLogReader(),
              () -> "Family " + desc.getName() + " doesn't have commit-log reader"));
    }
    return Optional.empty();
  }

  private CommitLogReader commitLogReader() {
    if (commitLogReader == null) {
      commitLogReader = commitLogReaderFactory.create();
    }
    return commitLogReader;
  }

  /**
   * Retrieve batch reader of this family.
   *
   * @return optional {@link BatchLogObservable} of this family
   */
  public Optional<BatchLogObservable> getBatchObservable() {
    if (desc.getAccess().canReadBatchSnapshot() || desc.getAccess().canReadBatchUpdates()) {

      return Optional.of(
          Objects.requireNonNull(
              batchObservable(),
              () -> "Family " + desc.getName() + " doesn't have batch observable"));
    }
    return Optional.empty();
  }

  private BatchLogObservable batchObservable() {
    if (batchObservable == null) {
      batchObservable = batchObservableFactory.create();
    }
    return batchObservable;
  }

  /**
   * Retrieve a random access reader. Empty if this attribute family is not a random access.
   *
   * @return optional {@link RandomAccessReader} of this family
   */
  public Optional<RandomAccessReader> getRandomAccessReader() {
    if (desc.getAccess().canRandomRead()) {
      return Optional.of(
          Objects.requireNonNull(
              randomAccessReader(),
              () -> "Family " + desc.getName() + " doesn't have random access reader"));
    }
    return Optional.empty();
  }

  private RandomAccessReader randomAccessReader() {
    if (randomAccessReader == null) {
      randomAccessReader = randomAccessReaderFactory.create();
    }
    return randomAccessReader;
  }

  /**
   * Retrieve cached view. Empty if the attribute family cannot create cached view.
   *
   * @return optional {@link CachedView} of this family
   */
  public Optional<CachedView> getCachedView() {
    if (desc.getAccess().canCreateCachedView()) {
      return Optional.of(
          Objects.requireNonNull(
              cachedView(), () -> "Family " + desc.getName() + " cannot create cached view"));
    }
    return Optional.empty();
  }

  private CachedView cachedView() {
    if (cachedView == null) {
      cachedView = cachedViewFactory.create();
    }
    return cachedView;
  }

  /**
   * Retrieve optional name of source attribute family, if this is replica. The source might not be
   * explicitly specified (in which case this method returns {@code Optional.empty()} and the source
   * is determined automatically.
   *
   * @return optional specified source family
   */
  public Optional<String> getSource() {
    return desc.getSource();
  }
}
