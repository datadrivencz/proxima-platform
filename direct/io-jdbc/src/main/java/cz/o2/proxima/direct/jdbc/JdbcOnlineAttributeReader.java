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

import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.randomaccess.RandomOffset;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.util.Pair;
import java.io.IOException;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcOnlineAttributeReader extends AbstractStorage implements RandomAccessReader {

  private final JdbcDataAccessor accessor;
  private final SqlStatementFactory sqlStatementFactory;

  public JdbcOnlineAttributeReader(
      JdbcDataAccessor accessor,
      SqlStatementFactory sqlStatementFactory,
      EntityDescriptor entityDesc,
      URI uri) {
    super(entityDesc, uri);
    this.accessor = accessor;
    this.sqlStatementFactory = sqlStatementFactory;
  }

  @Override
  public RandomOffset fetchOffset(Listing type, String key) {
    return null;
  }

  @Override
  public <T> Optional<KeyValue<T>> get(
      String key, String attribute, AttributeDescriptor<T> desc, long stamp) {
    try (PreparedStatement statement =
            sqlStatementFactory.get(accessor.getDataSource(), desc, key);
        ResultSet result = statement.executeQuery()) {
      log.debug("Executed statement {}", statement);
      if (!result.next()) {
        return Optional.empty();
      } else {
        return Optional.of(
            KeyValue.of(
                getEntityDescriptor(),
                desc,
                key,
                desc.getName(),
                new Offsets.Raw(accessor.getResultConverter().getKeyFromResult(result)),
                result.getString(desc.getName()).getBytes(),
                accessor.getResultConverter().getTimestampFromResult(result)));
      }
    } catch (SQLException e) {
      log.error("Error during query execution: {}", e.getMessage(), e);
      // @TODO: Maybe re-throw exception in case of SQLSyntaxErrorException?
      return Optional.empty();
    }
  }

  @Override
  public void scanWildcardAll(
      String key,
      @Nullable RandomOffset offset,
      long stamp,
      int limit,
      Consumer<KeyValue<?>> consumer) {

    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public <T> void scanWildcard(
      String key,
      AttributeDescriptor<T> wildcard,
      @Nullable RandomOffset offset,
      long stamp,
      int limit,
      Consumer<KeyValue<T>> consumer) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void listEntities(
      @Nullable RandomOffset offset, int limit, Consumer<Pair<RandomOffset, String>> consumer) {
    try (PreparedStatement statement =
            sqlStatementFactory.list(accessor.getDataSource(), offset, limit);
        ResultSet resultSet = statement.executeQuery()) {
      log.debug("Executed statement {}", statement);
      while (resultSet.next()) {
        String key = accessor.getResultConverter().getKeyFromResult(resultSet);
        consumer.accept(Pair.of(new Offsets.Raw(key), key));
      }
    } catch (SQLException e) {
      log.error(
          "Error during query execution: {}.",
          e.getMessage(),
          e); // @TODO: what to do in this case?
    }
  }

  @Override
  public void close() throws IOException {}
}
