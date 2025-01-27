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
package cz.o2.proxima.direct.io.s3;

import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.io.blob.BlobStorageAccessor;
import cz.o2.proxima.direct.io.bulkfs.FileSystem;
import java.util.Optional;

/** A {@link DataAccessor} for gcloud storage. */
class S3Accessor extends BlobStorageAccessor {

  private static final long serialVersionUID = 1L;

  private S3FileSystem fs;

  public S3Accessor(AttributeFamilyDescriptor family) {
    super(family);
  }

  @Override
  public FileSystem getTargetFileSystem() {
    return fs;
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    this.fs = initFs(context);
    return Optional.of(new BulkS3Writer(this, context));
  }

  @Override
  public Optional<BatchLogReader> getBatchLogReader(Context context) {
    this.fs = initFs(context);
    return Optional.of(new S3LogReader(this, context));
  }

  S3FileSystem initFs(Context context) {
    if (fs == null) {
      return new S3FileSystem(this, context);
    }
    return fs;
  }
}
