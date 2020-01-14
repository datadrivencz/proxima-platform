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
package cz.o2.proxima.direct.jdbc;

import com.google.common.base.Preconditions;
import cz.o2.proxima.direct.core.AbstractOnlineAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcOnlineAttributeWriter extends AbstractOnlineAttributeWriter {
  private final JdbcDataAccessor accessor;
  private final SqlStatementFactory sqlStatementFactory;

  protected JdbcOnlineAttributeWriter(
      JdbcDataAccessor accessor,
      SqlStatementFactory sqlStatementFactory,
      EntityDescriptor entityDesc,
      URI uri) {
    super(entityDesc, uri);
    this.accessor = accessor;
    this.sqlStatementFactory = sqlStatementFactory;
  }

  @Override
  public void close() {
    // accessor.getDataSource().close();@TODO
  }

  @Override
  public void write(StreamElement data, CommitCallback statusCallback) {
    Preconditions.checkArgument(!data.getKey().isEmpty(), "Key should not be empty.");
    try (PreparedStatement statement = sqlStatementFactory.update(accessor.getDataSource(), data)) {
      log.debug("Executing statement {}", statement);
      int result = statement.executeUpdate();
      statusCallback.commit(result != 0, null);
    } catch (SQLException e) {
      log.error("Error in writing data {}", e.getMessage(), e);
      statusCallback.commit(false, e);
    }
  }
}
