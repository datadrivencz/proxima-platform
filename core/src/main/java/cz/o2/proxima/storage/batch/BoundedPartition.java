/**
 * Copyright 2017 O2 Czech Republic, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.storage.batch;

import cz.o2.proxima.storage.Partition;

/**
 * Bounded implementation of {@code Partition}.
 */
public class BoundedPartition implements Partition {

  private final int id;
  private final long size;

  public BoundedPartition(int id) {
    this(id, -1L);
  }

  public BoundedPartition(int id, long size) {
    this.size = size;
    this.id = id;
  }

  @Override
  public boolean isBounded() {
    return true;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public long size() {
    return size;
  }

}
