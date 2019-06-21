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
import com.google.bigtable.admin.v2.GetClusterRequest;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.PostgresDatabase;
import com.spotify.autoscaler.db.PostgresDatabaseTest;
import com.spotify.metrics.core.SemanticMetricRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
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

  private static Map<String, FakeBTCluster> simulatedClusters = new HashMap<>();

  static {
    try (Stream<Path> list = Files.list(Path.of(FakeBTCluster.METRICS_PATH))) {
      list.forEach(
          path -> {
            final BigtableCluster cluster =
                FakeBTCluster.getClusterBuilderForFilePath(path)
                    .minNodes(5)
                    .maxNodes(1000)
                    .cpuTarget(0.8)
                    .build();
            simulatedClusters.put(
                cluster.clusterName(), new FakeBTCluster(new TimeSupplier(), cluster));
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setUp() throws IOException {
    initMocks(this);
    when(registry.meter(any())).thenReturn(new Meter());
    db = initDatabase(registry);
    when(bigtableSession.getInstanceAdminClient()).thenReturn(bigtableInstanceClient);

    when(bigtableInstanceClient.getCluster(any()))
        .thenAnswer(
            invocationOnMock -> {
              final GetClusterRequest getClusterReq = invocationOnMock.getArgument(0);
              if (getClusterReq != null) {
                final FakeBTCluster fakeBTCluster = simulatedClusters.get(getClusterReq.getName());
                return Cluster.newBuilder()
                    .setName(fakeBTCluster.getCluster().clusterName())
                    .setServeNodes(fakeBTCluster.getNumberOfNodes())
                    .build();
              } else {
                return Cluster.newBuilder().build();
              }
            });

    when(bigtableInstanceClient.updateCluster(any()))
        .thenAnswer(
            invocationOnMock -> {
              final Cluster cluster = invocationOnMock.getArgument(0);
              int newSize = cluster.getServeNodes();
              final FakeBTCluster fakeBTCluster = simulatedClusters.get(cluster.getName());
              if (fakeBTCluster != null) {
                fakeBTCluster.setNumberOfNodes(newSize);
              }
              return null;
            });
    simulatedClusters.forEach((k, v) -> v.setNumberOfNodes(100));
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.00001);
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

  private static PostgresDatabase initDatabase(final SemanticMetricRegistry registry) {
    final PostgresDatabase database = PostgresDatabaseTest.getPostgresDatabase();
    simulatedClusters.forEach(
        (k, v) -> {
          final BigtableCluster cluster = v.getCluster();
          database.deleteBigtableCluster(
              cluster.projectId(), cluster.instanceId(), cluster.clusterId());
          database.insertBigtableCluster(cluster);
        });

    return database;
  }

  @Test
  public void testWeDontResizeTooOften() throws IOException {
    // To give the cluster a chance to settle in, don't resize too often

    // first time we get the last event from the DB we get nothing
    // then we get approximately 8 minutes

    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.00001);
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.7);

    final FakeBTCluster fakeBTCluster = getDefaultFakeBTCluster();
    final BigtableCluster cluster = fakeBTCluster.getCluster();
    runJobAndAssertNewSize(fakeBTCluster, cluster, 88, Instant::now);

    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.5);
    final BigtableCluster updatedCluster =
        db.getBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId()).get();

    runJobAndAssertNewSize(
        fakeBTCluster, updatedCluster, 88, () -> Instant.now().plus(Duration.ofMinutes(8)));
  }

  private FakeBTCluster getDefaultFakeBTCluster() {
    final String clusterName =
        String.format("projects/%s/instances/%s/clusters/%s", "project", "instance", "cluster");
    return simulatedClusters.get(clusterName);
  }

  @Test
  public void testSmallResizesDontHappenTooOften() throws IOException {
    // To avoid oscillating, don't do small size changes too often
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.7);

    final FakeBTCluster fakeBTCluster = getDefaultFakeBTCluster();
    final BigtableCluster cluster = fakeBTCluster.getCluster();
    runJobAndAssertNewSize(fakeBTCluster, cluster, 88, Instant::now);

    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.78);
    final BigtableCluster updatedCluster =
        db.getBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId()).get();

    runJobAndAssertNewSize(
        fakeBTCluster, updatedCluster, 88, () -> Instant.now().plus(Duration.ofMinutes(100)));
  }

  private void runJobAndAssertNewSize(
      final FakeBTCluster fakeBTCluster,
      final BigtableCluster cluster,
      int expectedSize,
      final Supplier<Instant> timeSource)
      throws IOException {
    final AutoscaleJob job =
        new AutoscaleJob(
            bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, timeSource);
    job.run();
    assertEquals(expectedSize, fakeBTCluster.getNumberOfNodes());
  }

  @Test
  public void testSmallResizesHappenEventually() throws IOException {
    // To avoid oscillating, don't do small size changes too often
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.7);

    final FakeBTCluster fakeBTCluster = getDefaultFakeBTCluster();
    final BigtableCluster cluster = fakeBTCluster.getCluster();
    runJobAndAssertNewSize(fakeBTCluster, cluster, 88, Instant::now);

    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.78);
    final BigtableCluster updatedCluster =
        db.getBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId()).get();

    runJobAndAssertNewSize(
        fakeBTCluster, updatedCluster, 86, () -> Instant.now().plus(Duration.ofMinutes(480)));
  }

  @Test
  public void randomDataTest() throws IOException {
    // This test is useful to see that we don't get stuck at any point, for example
    // there is no Connection leak.
    final Random random = new Random();
    final Instant start = Instant.now();

    final TimeSupplier timeSupplier = new TimeSupplier();
    timeSupplier.setTime(start);

    final FakeBTCluster fakeBTCluster = getDefaultFakeBTCluster();
    final BigtableCluster cluster = fakeBTCluster.getCluster();

    testThroughTime(
        cluster,
        timeSupplier,
        Duration.ofSeconds(300),
        512,
        random::nextDouble,
        random::nextDouble,
        ignored -> assertTrue(true));
  }

  @Test
  public void simulateCluster() throws IOException {
    final Instant start = Instant.parse("2019-06-16T07:19:00Z");

    final FakeBTCluster fakeBTCluster = getDefaultFakeBTCluster();
    final TimeSupplier timeSupplier = (TimeSupplier) fakeBTCluster.getTimeSource();
    timeSupplier.setTime(start);

    final BigtableCluster cluster = fakeBTCluster.getCluster();
    final int initialNodeCount = fakeBTCluster.getMetricsForNow().nodeCount().intValue();
    fakeBTCluster.setNumberOfNodes(initialNodeCount);
    bigtableInstanceClient.updateCluster(
        Cluster.newBuilder()
            .setName(cluster.clusterName())
            .setServeNodes(initialNodeCount)
            .build());

    testThroughTime(
        cluster,
        timeSupplier,
        Duration.ofSeconds(300),
        280,
        fakeBTCluster::getCPU,
        fakeBTCluster::getStorage,
        ignored -> assertTrue(fakeBTCluster.getCPU() < cluster.cpuTarget() + 0.1d));
  }

  private void testThroughTime(
      final BigtableCluster cluster,
      final TimeSupplier timeSupplier,
      final Duration period,
      final int repetition,
      final Supplier<Double> cpuSupplier,
      final Supplier<Double> diskUtilSupplier,
      final Consumer<Void> assertion)
      throws IOException {

    Instant now = timeSupplier.get();
    for (int i = 0; i < repetition; ++i) {
      now = now.plus(period);
      timeSupplier.setTime(now);
      AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, cpuSupplier.get());
      AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, diskUtilSupplier.get());

      final AutoscaleJob job =
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
