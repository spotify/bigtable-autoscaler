/*-
 * -\-\-
 * bigtable-autoscaler
 * --
 * Copyright (C) 2018 Spotify AB
 * --
 * The Apache Software License, Version 2.0
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.autoscaler.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

public class PostgresDatabaseIT {
  @ClassRule
  public static PostgreSQLContainer pg = new PostgreSQLContainer();

  SemanticMetricRegistry registry;
  PostgresDatabase db;
  String projectId = "test-project";
  String instanceId = "test-instance";
  String clusterId = "test-cluster";

  private BigtableCluster testCluster() {
    return new BigtableClusterBuilder()
        .projectId(projectId)
        .instanceId(instanceId)
        .clusterId(clusterId)
        .minNodes(10)
        .maxNodes(100)
        .cpuTarget(0.8)
        .overloadStep(10)
        .enabled(true)
        .build();
  }

  @Before
  public void setup() throws SQLException, IOException {

    Connection connection = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
    // create the table
    String table = Resources.toString(Resources.getResource("autoscale.sql"), Charsets.UTF_8);
    PreparedStatement createTable = connection.prepareStatement(table);
    createTable.executeUpdate();

    // insert a test cluster
    Config config = ConfigFactory.empty()
        .withValue("jdbcUrl", ConfigValueFactory.fromAnyRef(pg.getJdbcUrl()))
        .withValue("username", ConfigValueFactory.fromAnyRef(pg.getUsername()))
        .withValue("password", ConfigValueFactory.fromAnyRef(pg.getPassword()));
    registry = mock(SemanticMetricRegistry.class);
    db = new PostgresDatabase(config, registry);
  }

  @After
  public void tearDown() {
    db.getBigtableClusters()
        .stream()
        .forEach(cluster -> db.deleteBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId()));
    db.close();
  }

  @Test
  public void testSetupAndConnection() {
    assertEquals(0, db.getBigtableClusters().size());
  }

  @Test
  public void setLastChangeTest() {
    db.insertBigtableCluster(testCluster());

    Instant now = Instant.now();
    db.setLastChange(projectId, instanceId, clusterId, now);
    Optional<BigtableCluster> btcluster = db.getBigtableCluster(projectId, instanceId, clusterId);
    assertTrue(btcluster.isPresent());
    Optional<Instant> alsoNow = btcluster.get().lastChange();
    assertTrue(alsoNow.isPresent());
    assertEquals(now, alsoNow.get());
  }

  @Test
  public void insert() {
    db.insertBigtableCluster(testCluster());
    assertEquals(ImmutableList.of(testCluster()), db.getBigtableClusters());
  }

  @Test
  public void update() {
    db.insertBigtableCluster(BigtableClusterBuilder.from(testCluster()).minNodes(5).build());
    assertNotEquals(ImmutableList.of(testCluster()), db.getBigtableClusters());
    db.updateBigtableCluster(testCluster());
    assertEquals(ImmutableList.of(testCluster()), db.getBigtableClusters());
  }

  @Test
  public void enable() {
    db.insertBigtableCluster(BigtableClusterBuilder.from(testCluster()).enabled(false).build());
    assertNotEquals(ImmutableList.of(testCluster()), db.getBigtableClusters());
    db.updateBigtableCluster(testCluster());
    assertEquals(ImmutableList.of(testCluster()), db.getBigtableClusters());
  }

  @Test
  public void delete() {
    db.insertBigtableCluster(testCluster());
    db.deleteBigtableCluster(projectId, instanceId, clusterId);
    assertEquals(0, db.getBigtableClusters().size());
  }

  @Test
  public void testInsertedAndRetrievedClustersAreEquivalent() throws Exception {

    BigtableCluster cluster = BigtableClusterBuilder.from(testCluster())
        .overloadStep(Optional.ofNullable(null))
        .build();

    db.insertBigtableCluster(cluster);

    BigtableCluster retrievedCluster = db.getBigtableCluster(cluster.projectId(), cluster.instanceId(),
        cluster.clusterId()).orElseThrow(() -> new RuntimeException("Inserted cluster not present!!"));

    assertEquals(cluster, retrievedCluster);

  }
}
