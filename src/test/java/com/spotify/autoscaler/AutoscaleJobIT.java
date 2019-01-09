/*-
 * -\-\-
 * bigtable-autoscaler
 * --
 * Copyright (C) 2018 Spotify AB
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

package com.spotify.autoscaler;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.codahale.metrics.Meter;
import com.google.bigtable.admin.v2.Cluster;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.db.PostgresDatabase;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.testcontainers.containers.PostgreSQLContainer;

@RunWith(MockitoJUnitRunner.Silent.class)
public class AutoscaleJobIT {
  @ClassRule
  public static PostgreSQLContainer pg = new PostgreSQLContainer();

  @Mock
  BigtableSession bigtableSession;

  @Mock
  BigtableInstanceClient bigtableInstanceClient;

  @Mock
  StackdriverClient stackdriverClient;

  @Mock
  SemanticMetricRegistry registry;

  @Mock
  ClusterStats clusterStats;

  Database db;
  AutoscaleJob job;
  BigtableCluster cluster = new BigtableClusterBuilder()
      .projectId("project").instanceId("instance").clusterId("cluster")
      .cpuTarget(0.8).maxNodes(500).minNodes(5).overloadStep(100).enabled(true).exists(true).build();
  int newSize;

  @Before
  public void setUp() throws IOException, SQLException {
    initMocks(this);
    when(registry.meter(any())).thenReturn(new Meter());
    db = initDatabase(cluster, registry);
    when(bigtableSession.getInstanceAdminClient()).thenReturn(bigtableInstanceClient);
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.00001);
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, 100);
    job = new AutoscaleJob(bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, () -> Instant.now());
    when(bigtableInstanceClient.updateCluster(any()))
        .thenAnswer(
            invocationOnMock -> {
              newSize = ((Cluster) invocationOnMock.getArgument(0)).getServeNodes();
              AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, newSize);
              return null;
            });
  }

  private static Database initDatabase(BigtableCluster cluster, SemanticMetricRegistry registry) throws SQLException, IOException {
    Config config = ConfigFactory.empty()
        .withValue("jdbcUrl", ConfigValueFactory.fromAnyRef(pg.getJdbcUrl()))
        .withValue("username", ConfigValueFactory.fromAnyRef(pg.getUsername()))
        .withValue("password", ConfigValueFactory.fromAnyRef(pg.getPassword()));

    // create the tables in the database
    Connection connection = DriverManager
        .getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
    String table = Resources.toString(Resources.getResource("schema.sql"), Charsets.UTF_8);
    PreparedStatement createTable = connection.prepareStatement(table);
    createTable.executeUpdate();

    Database db = new PostgresDatabase(config, registry);
    db.deleteBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId());
    db.insertBigtableCluster(cluster);
    return db;
  }

  @Test
  public void testWeDontResizeTooOften() throws IOException {
    // To give the cluster a chance to settle in, don't resize too often

    // first time we get the last event from the DB we get nothing
    // then we get approximately 8 minutes
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.7);
    job.run();
    assertEquals(88, newSize);

    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.5);
    BigtableCluster updatedCluster = db.getBigtableCluster(cluster.projectId(),
        cluster.instanceId(), cluster.clusterId()).get();
    job = new AutoscaleJob(bigtableSession, stackdriverClient, updatedCluster, db, registry, clusterStats,
        () -> Instant.now().plus(Duration.ofMinutes(8)));
    job.run();
    assertEquals(88, newSize);
  }

  @Test
  public void testSmallResizesDontHappenTooOften() throws IOException {
    // To avoid oscillating, don't do small size changes too often
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.7);
    job.run();
    assertEquals(88, newSize);

    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.78);
    BigtableCluster updatedCluster = db.getBigtableCluster(cluster.projectId(),
        cluster.instanceId(), cluster.clusterId()).get();
    job = new AutoscaleJob(bigtableSession, stackdriverClient, updatedCluster, db, registry, clusterStats,
        () -> Instant.now().plus(Duration.ofMinutes(100)));
    job.run();
    assertEquals(88, newSize);
  }

  @Test
  public void testSmallResizesHappenEventually() throws IOException {
    // To avoid oscillating, don't do small size changes too often
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.7);
    job.run();
    assertEquals(88, newSize);

    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.78);
    BigtableCluster updatedCluster = db.getBigtableCluster(cluster.projectId(),
        cluster.instanceId(), cluster.clusterId()).get();
    job = new AutoscaleJob(bigtableSession, stackdriverClient, updatedCluster, db, registry, clusterStats,
        () -> Instant.now().plus(Duration.ofMinutes(480)));
    job.run();
    assertEquals(86, newSize);
  }

  @Test
  public void stressTest() throws IOException {
    // This test is useful to see that we don't get stuck at any point, for example
    // there is no Connection leak.
    Random random = new Random();
    Instant start = Instant.now();

    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, 100);

    for (int i = 0; i < 512; ++i) {
      AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, random.nextDouble());
      AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, random.nextDouble());
      final Instant now = start.plus(Duration.ofSeconds(300));

      job = new AutoscaleJob(
          bigtableSession,
          stackdriverClient,
          cluster,
          db,
          registry,
          clusterStats,
          () -> now);
      job.run();
    }
  }
}
