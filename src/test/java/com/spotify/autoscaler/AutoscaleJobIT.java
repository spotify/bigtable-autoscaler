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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.codahale.metrics.Meter;
import com.google.bigtable.admin.v2.Cluster;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.PostgresDatabase;
import com.spotify.autoscaler.db.PostgresDatabaseTest;
import com.spotify.autoscaler.util.ErrorCode;
import com.spotify.metrics.core.SemanticMetricRegistry;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class AutoscaleJobIT {

  @Mock BigtableSession bigtableSession;

  @Mock BigtableInstanceClient bigtableInstanceClient;

  @Mock StackdriverClient stackdriverClient;

  @Mock SemanticMetricRegistry registry;

  @Mock ClusterStats clusterStats;

  PostgresDatabase db;
  AutoscaleJob job;
  BigtableCluster cluster =
      new BigtableClusterBuilder()
          .projectId("project")
          .instanceId("instance")
          .clusterId("cluster")
          .cpuTarget(0.8)
          .maxNodes(500)
          .minNodes(5)
          .overloadStep(100)
          .enabled(true)
          .errorCode(Optional.of(ErrorCode.OK))
          .build();
  int newSize;

  private static Map<Cluster, FakeBTCluster> simulatedClusters = new HashMap<>();

  @Before
  public void setUp() throws IOException, SQLException {
    initMocks(this);
    when(registry.meter(any())).thenReturn(new Meter());
    db = initDatabase(cluster, registry);
    when(bigtableSession.getInstanceAdminClient()).thenReturn(bigtableInstanceClient);
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.00001);
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, 100);
    job =
        new AutoscaleJob(
            bigtableSession,
            stackdriverClient,
            cluster,
            db,
            registry,
            clusterStats,
            () -> Instant.now());
    when(bigtableInstanceClient.updateCluster(any()))
        .thenAnswer(
            invocationOnMock -> {
              final Cluster cluster = invocationOnMock.getArgument(0);
              newSize = cluster.getServeNodes();
              AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, newSize);
              final FakeBTCluster fakeBTCluster = simulatedClusters.get(cluster);
              if (fakeBTCluster != null) {
                fakeBTCluster.setNumberOfNodes(newSize);
              }
              return null;
            });
  }

  @After
  public void tearDown() {
    db.getBigtableClusters()
        .stream()
        .forEach(
            cluster ->
                db.deleteBigtableCluster(
                    cluster.projectId(), cluster.instanceId(), cluster.clusterId()));
    db.close();
  }

  private static PostgresDatabase initDatabase(
      final BigtableCluster cluster, final SemanticMetricRegistry registry) {
    final PostgresDatabase database = PostgresDatabaseTest.getPostgresDatabase();
    database.deleteBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId());
    database.insertBigtableCluster(cluster);
    return database;
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
    final BigtableCluster updatedCluster =
        db.getBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId()).get();
    job =
        new AutoscaleJob(
            bigtableSession,
            stackdriverClient,
            updatedCluster,
            db,
            registry,
            clusterStats,
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
    final BigtableCluster updatedCluster =
        db.getBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId()).get();
    job =
        new AutoscaleJob(
            bigtableSession,
            stackdriverClient,
            updatedCluster,
            db,
            registry,
            clusterStats,
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
    final BigtableCluster updatedCluster =
        db.getBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId()).get();
    job =
        new AutoscaleJob(
            bigtableSession,
            stackdriverClient,
            updatedCluster,
            db,
            registry,
            clusterStats,
            () -> Instant.now().plus(Duration.ofMinutes(480)));
    job.run();
    assertEquals(86, newSize);
  }

  @Test
  public void randomDataTest() throws IOException {
    // This test is useful to see that we don't get stuck at any point, for example
    // there is no Connection leak.
    final Random random = new Random();
    final Instant start = Instant.now();

    final TimeSupplier timeSupplier = new TimeSupplier();
    timeSupplier.setTime(start);

    testThroughTime(
        timeSupplier,
        Duration.ofSeconds(300),
        512,
        random::nextDouble,
        random::nextDouble,
        ignored -> assertTrue(true));
  }

  @Test
  public void simulateCluster() throws IOException {
    final Instant start = Instant.parse("2019-06-09T13:31:00Z");
    final TimeSupplier timeSupplier = new TimeSupplier();
    timeSupplier.setTime(start);

    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, 100);

    final FakeBTCluster fakeBTCluster = new FakeBTCluster(timeSupplier, cluster);
    fakeBTCluster.setNumberOfNodes(100);

    simulatedClusters.put(
        Cluster.newBuilder().setName(cluster.clusterName()).build(), fakeBTCluster);

    testThroughTime(
        timeSupplier,
        Duration.ofSeconds(300),
        280,
        fakeBTCluster::getCPU,
        fakeBTCluster::getStorage,
        ignored -> assertTrue(fakeBTCluster.getCPU() < 0.9d));
  }

  private void testThroughTime(
      final TimeSupplier timeSupplier,
      final Duration period,
      final int repetition,
      final Supplier<Double> cpuSupplier,
      final Supplier<Double> diskUtilSupplier,
      final Consumer<Void> assertion)
      throws IOException {

    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, 100);

    Instant now = timeSupplier.get();
    for (int i = 0; i < repetition; ++i) {
      now = now.plus(period);
      timeSupplier.setTime(now);
      AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, cpuSupplier.get());
      AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, diskUtilSupplier.get());

      job =
          new AutoscaleJob(
              bigtableSession,
              stackdriverClient,
              cluster,
              db,
              registry,
              clusterStats,
              timeSupplier);
      job.run();
      assertion.accept(null);
    }
  }
}
