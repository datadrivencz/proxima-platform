/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.sql.proto.Gateway.GatewayDetails;
import cz.o2.proxima.direct.sql.proto.User.UserDetails;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class SelectTest {

  Connection connection;
  CalciteConnection calciteConnection;
  SchemaPlus rootSchema;

  @Before
  public void setUp() throws SQLException {
    URL model = getClass().getClassLoader().getResource("calcite.json");
    connection = DriverManager.getConnection("jdbc:calcite:model=" + model.getFile());
    calciteConnection = connection.unwrap(CalciteConnection.class);
    rootSchema = calciteConnection.getRootSchema();
  }

  @After
  public void tearDown() throws SQLException {
    calciteConnection.close();
  }

  @Test
  public void testSelect() throws SQLException {
    SchemaPlus proxima = calciteConnection.getRootSchema().getSubSchema("PROXIMA");
    EntityTable gateway = (EntityTable) proxima.getTable("GATEWAY");
    Repository repo = gateway.getRepo();
    DirectDataOperator direct = gateway.getDirect();
    EntityDescriptor gatewayDesc = repo.getEntity("gateway");
    Regular<GatewayDetails> details = Regular.of(gatewayDesc, gatewayDesc.getAttribute("details"));
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(details));
    writer.write(
        details.upsert("gw", Instant.now(), GatewayDetails.newBuilder().setName("gw").build()),
        (succ, exc) -> {});
    writer.write(
        details.upsert("gw2", Instant.now(), GatewayDetails.newBuilder().setName("gw2").build()),
        (succ, exc) -> {});
    try (Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select count(*) c from proxima.gateway")) {

      assertTrue(resultSet.next());
      assertEquals(2L, resultSet.getLong("c"));
    }

    try (Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select key from proxima.gateway")) {

      List<String> keys = new ArrayList<>();
      while (resultSet.next()) {
        keys.add(resultSet.getString("key"));
      }
      assertEquals(List.of("gw", "gw2"), keys);
    }

    try (Statement statement = calciteConnection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("select count(*) c from proxima.gateway where key = 'gw'")) {

      assertTrue(resultSet.next());
      assertEquals(1L, resultSet.getLong("c"));
    }

    try (Statement statement = calciteConnection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select count(*) c from proxima.gateway where gateway.details.name = 'gw'")) {

      assertTrue(resultSet.next());
      assertEquals(1L, resultSet.getLong("c"));
    }
  }

  @Test
  public void testSelectMultiAttr() throws SQLException {
    SchemaPlus proxima = calciteConnection.getRootSchema().getSubSchema("PROXIMA");
    EntityTable gateway = (EntityTable) proxima.getTable("GATEWAY");
    Repository repo = gateway.getRepo();
    DirectDataOperator direct = gateway.getDirect();
    EntityDescriptor gatewayDesc = repo.getEntity("gateway");
    Regular<GatewayDetails> gatewayDetails =
        Regular.of(gatewayDesc, gatewayDesc.getAttribute("details"));
    Regular<String> owner = Regular.of(gatewayDesc, gatewayDesc.getAttribute("owner"));
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(gatewayDetails));
    writer.write(
        gatewayDetails.upsert(
            "gw", Instant.now(), GatewayDetails.newBuilder().setName("name").build()),
        (succ, exc) -> {});
    writer.write(owner.upsert("gw", Instant.now(), "user1"), (succ, exc) -> {});
    try (Statement statement = calciteConnection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select key, g.details.name n, owner o from proxima.gateway as g")) {

      assertTrue(resultSet.next());
      assertEquals("gw", resultSet.getString("key"));
      assertEquals("name", resultSet.getString("n"));
      assertEquals("user1", resultSet.getString("o"));
    }
  }

  // FIXME
  @Ignore
  @Test
  public void testSelectJoin() throws SQLException {
    SchemaPlus proxima = calciteConnection.getRootSchema().getSubSchema("PROXIMA");
    EntityTable gateway = (EntityTable) proxima.getTable("GATEWAY");
    Repository repo = gateway.getRepo();
    DirectDataOperator direct = gateway.getDirect();
    EntityDescriptor gatewayDesc = repo.getEntity("gateway");
    EntityDescriptor userDesc = repo.getEntity("user");
    Regular<GatewayDetails> gatewayDetails =
        Regular.of(gatewayDesc, gatewayDesc.getAttribute("details"));
    Regular<String> owner = Regular.of(gatewayDesc, gatewayDesc.getAttribute("owner"));
    Regular<UserDetails> userDetails = Regular.of(userDesc, userDesc.getAttribute("details"));
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(gatewayDetails));
    writer.write(
        gatewayDetails.upsert(
            "gw", Instant.now(), GatewayDetails.newBuilder().setName("gw").build()),
        (succ, exc) -> {});
    writer.write(owner.upsert("gw", Instant.now(), "user1"), (succ, exc) -> {});
    writer.write(
        gatewayDetails.upsert(
            "gw2", Instant.now(), GatewayDetails.newBuilder().setName("gw2").build()),
        (succ, exc) -> {});
    writer = Optionals.get(direct.getWriter(userDetails));
    writer.write(
        userDetails.upsert(
            "user1", Instant.now(), UserDetails.newBuilder().setName("user").build()),
        (succ, exc) -> {});
    try (Statement statement = calciteConnection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select u.key, u.details.name from proxima.gateway as g join proxima.user as u on g.owner = u.key")) {

      assertTrue(resultSet.next());
      assertEquals("user", resultSet.getString(0));
    }
  }
}
