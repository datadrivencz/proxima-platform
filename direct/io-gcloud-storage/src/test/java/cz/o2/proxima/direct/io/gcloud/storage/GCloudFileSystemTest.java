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
package cz.o2.proxima.direct.io.gcloud.storage;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.direct.io.bulkfs.FileFormat;
import cz.o2.proxima.direct.io.bulkfs.NamingConvention;
import cz.o2.proxima.direct.io.bulkfs.Path;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

/** Test {@link GCloudFileSystem}. */
public class GCloudFileSystemTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor entity = repo.getEntity("gateway");
  private final GCloudStorageAccessor accessor =
      new GCloudStorageAccessor(TestUtils.createTestFamily(entity, URI.create("gs://bucket/path")));
  private final Map<String, Blob> blobs = new HashMap<>();
  private final FileFormat format = FileFormat.blob(true);
  private final NamingConvention naming =
      NamingConvention.defaultConvention(Duration.ofHours(1), "prefix", format.fileSuffix());
  private final GCloudFileSystem fs =
      new GCloudFileSystem(accessor) {
        @Override
        Storage client() {
          return mockStorage();
        }
      };

  private Storage mockStorage() {
    Storage ret = mock(Storage.class);
    doAnswer(
            invocation -> {
              String bucket = invocation.getArguments()[0].toString();
              assertEquals("bucket", bucket);
              BlobListOption option = (BlobListOption) invocation.getArguments()[1];
              String tmp = option.toString();
              int valueIndex = tmp.indexOf("value=");
              String prefix = valueIndex > 0 ? tmp.substring(valueIndex + 6, tmp.length() - 1) : "";
              return asPage(
                  blobs.entrySet().stream()
                      .filter(entry -> entry.getKey().startsWith(prefix))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            })
        .when(ret)
        .list(anyString(), any());
    return ret;
  }

  private Page<Blob> asPage(Map<String, Blob> blobs) {
    @SuppressWarnings("unchecked")
    Page<Blob> page = mock(Page.class);
    when(page.iterateAll()).thenReturn(blobs.values());
    return page;
  }

  private Blob mockBlob(String name) {
    Blob blob = mock(Blob.class);
    when(blob.getName()).thenReturn(name);
    return blob;
  }

  @Test
  public void testList() {
    long now = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      String name = "path" + naming.nameOf(now + 86400000L * i);
      Blob blob = mockBlob(name);
      blobs.put(name, blob);
    }
    List<Path> paths = fs.list().collect(Collectors.toList());
    assertEquals(100, paths.size());
  }

  @Test
  public void testListRange() {
    long now = 1500000000000L;
    for (int i = 0; i < 100; i++) {
      String name = "path" + naming.nameOf(now + 86400000L * i);
      Blob blob = mockBlob(name);
      blobs.put(name, blob);
    }
    List<Path> paths = fs.list(now, now + 1).collect(Collectors.toList());
    assertEquals(1, paths.size());
  }
}
