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
import static org.junit.Assert.assertFalse;
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
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.util.ErrorCode;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class AutoscaleJobTest {

  @Mock BigtableSession bigtableSession;

  @Mock BigtableInstanceClient bigtableInstanceClient;

  @Mock StackdriverClient stackdriverClient;

  @Mock Database db;

  @Mock SemanticMetricRegistry registry;

  @Mock ClusterStats clusterStats;

  private final int MIN_NODES = 6;
  private final int MAX_NODES = 500;
  private BigtableCluster cluster;
  private String projectId = "project";
  private String instanceId = "instance";
  private String clusterId = "cluster";

  private Optional<Integer> newSize = Optional.empty();
  private AutoscaleJob job;

  @Before
  public void setUp() throws IOException {
    cluster =
        new BigtableClusterBuilder()
            .projectId(projectId)
            .instanceId(instanceId)
            .clusterId(clusterId)
            .cpuTarget(0.8)
            .maxNodes(MAX_NODES)
            .minNodes(MIN_NODES)
            .overloadStep(100)
            .errorCode(Optional.of(ErrorCode.OK))
            .build();

    initMocks(this);
    when(registry.meter(any())).thenReturn(new Meter());
    when(bigtableSession.getInstanceAdminClient()).thenReturn(bigtableInstanceClient);
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.00001);
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, 100);
    job =
        new AutoscaleJob(
            bigtableSession,
            stackdriverClient,
            this.cluster,
            db,
            registry,
            clusterStats,
            Instant::now);
    when(bigtableInstanceClient.updateCluster(any()))
        .thenAnswer(
            invocationOnMock -> {
              newSize = Optional.of(((Cluster) invocationOnMock.getArgument(0)).getServeNodes());
              AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, newSize.get());
              return null;
            });
  }

  @Test
  public void testDiskConstraintOverridesCpuTargetedNodeCount() throws IOException {
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.8d);
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.4d);
    job.run();
    assertEquals(Optional.of(115), newSize);
  }

  @Test
  public void testDiskConstraintOverridesIfNotLoaded() throws IOException {
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.6d);
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.4d);
    job.run();
    assertEquals(Optional.of(86), newSize);
  }

  @Test
  public void testDiskConstraintDoesNotOverrideIfDesiredNodesAlreadyEnoughIfNotLoaded()
      throws IOException {
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.6d);
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.72d);
    job.run();
    assertEquals(Optional.of(90), newSize);
  }

  @Test
  public void testDiskConstraintDoesNotOverrideIfDesiredNodesAlreadyEnough() throws IOException {
    cluster = BigtableClusterBuilder.from(this.cluster).overloadStep(Optional.empty()).build();
    job =
        new AutoscaleJob(
            bigtableSession,
            stackdriverClient,
            this.cluster,
            db,
            registry,
            clusterStats,
            Instant::now);
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.8d);
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.96d);
    job.run();
    assertEquals(Optional.of(120), newSize);
  }

  @Test
  public void testResize() throws IOException {
    // Test that we resize to correct target size
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.6);
    job.run();
    assertEquals(Optional.of(75), newSize);
  }

  @Test
  public void testUpperBound() throws IOException {
    // Test that we don't go over maximum size
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, MAX_NODES - 20);
    SemanticMetricRegistry registry = new SemanticMetricRegistry();
    job =
        new AutoscaleJob(
            bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, Instant::now);
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.9);
    job.run();
    List<MetricId> metric =
        registry
            .getMeters()
            .keySet()
            .stream()
            .filter(meter -> meter.getTags().containsValue("overridden-desired-node-count"))
            .collect(Collectors.toList());

    assertEquals(1, metric.size());
    Map<String, String> tags = metric.get(0).getTags();
    assertEquals("max-nodes-constraint", tags.get("reason"));
    assertEquals(String.valueOf(MAX_NODES), tags.get("target-nodes"));
    assertEquals("540", tags.get("desired-nodes"));
    assertEquals(String.valueOf(MIN_NODES), tags.get("min-nodes"));
    assertEquals(String.valueOf(MAX_NODES), tags.get("max-nodes"));
    assertEquals(projectId, tags.get("project-id"));
    assertEquals(clusterId, tags.get("cluster-id"));
    assertEquals(instanceId, tags.get("instance-id"));
    assertEquals(Optional.of(MAX_NODES), newSize);
  }

  @Test
  public void testLowerBound() throws IOException {
    // Test that we don't go under minimum size
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, MIN_NODES + 1);
    SemanticMetricRegistry registry = new SemanticMetricRegistry();
    job =
        new AutoscaleJob(
            bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, Instant::now);
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.0001);
    job.run();
    List<MetricId> metric =
        registry
            .getMeters()
            .keySet()
            .stream()
            .filter(meter -> meter.getTags().containsValue("overridden-desired-node-count"))
            .collect(Collectors.toList());

    assertEquals(1, metric.size());
    Map<String, String> tags = metric.get(0).getTags();
    assertEquals("min-nodes-constraint", tags.get("reason"));
    assertEquals(String.valueOf(MIN_NODES), tags.get("target-nodes"));
    assertEquals("5", tags.get("desired-nodes"));
    assertEquals(String.valueOf(MIN_NODES), tags.get("min-nodes"));
    assertEquals(String.valueOf(MAX_NODES), tags.get("max-nodes"));
    assertEquals(projectId, tags.get("project-id"));
    assertEquals(clusterId, tags.get("cluster-id"));
    assertEquals(instanceId, tags.get("instance-id"));
    assertEquals(Optional.of(MIN_NODES), newSize);
  }

  @Test
  public void testHugeResizeOnOverload() throws IOException {
    // To give the cluster a chance to settle in, don't resize too often
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.95);
    job.run();
    assertEquals(Optional.of(200), newSize);
  }

  @Test(expected = RuntimeException.class)
  public void testJobCantRunTwice() throws IOException {
    job.run();
    job.run();
  }

  @Test
  public void testExponentialBackoffAfterConsecutiveFailures() {
    Instant now = Instant.now();

    BigtableCluster cluster =
        BigtableClusterBuilder.from(this.cluster)
            .consecutiveFailureCount(5) // 5 failures = 8 minutes
            .lastFailure(now)
            .build();

    job =
        new AutoscaleJob(
            bigtableSession,
            stackdriverClient,
            cluster,
            db,
            registry,
            clusterStats,
            () -> now.plusSeconds(300));
    assertTrue(job.shouldExponentialBackoff());
    job =
        new AutoscaleJob(
            bigtableSession,
            stackdriverClient,
            cluster,
            db,
            registry,
            clusterStats,
            () -> now.plusSeconds(1000));
    assertFalse(job.shouldExponentialBackoff());
  }

  @Test
  public void testNoExponentialBackoffAfterSuccess() {
    Instant now = Instant.now();

    BigtableCluster cluster =
        BigtableClusterBuilder.from(this.cluster)
            .consecutiveFailureCount(0) // last time succeeded
            .lastFailure(now)
            .build();

    job =
        new AutoscaleJob(
            bigtableSession,
            stackdriverClient,
            cluster,
            db,
            registry,
            clusterStats,
            () -> now.plusSeconds(50));
    assertFalse(job.shouldExponentialBackoff());
  }

  @Test
  public void testThatWeDontReduceClusterSizeTooFast() throws IOException {
    // Even if we're very over-provisioned, only reduce by 30 %
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.3);
    job.run();
    assertEquals(Optional.of(70), newSize);
  }

  @Test
  public void testThatWeSampleAShortPeriodIfWeHaveChangedSizeRecently() {
    assertEquals(Duration.ofMinutes(3), job.computeSamplingDuration(Duration.ofMinutes(8)));
    assertEquals(Duration.ofMinutes(12), job.computeSamplingDuration(Duration.ofMinutes(17)));
    assertEquals(Duration.ofMinutes(22), job.computeSamplingDuration(Duration.ofMinutes(27)));
    assertEquals(Duration.ofMinutes(59), job.computeSamplingDuration(Duration.ofMinutes(64)));
    assertEquals(Duration.ofMinutes(60), job.computeSamplingDuration(Duration.ofMinutes(100)));
  }

  @Test
  public void testWeResizeIfSizeConstraintsAreNotMet() throws IOException {
    int loadDelta = 10;
    BigtableCluster cluster =
        BigtableClusterBuilder.from(this.cluster)
            .loadDelta(loadDelta)
            .lastChange(Instant.now())
            .build();
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, MIN_NODES);
    job =
        new AutoscaleJob(
            bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, Instant::now);
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.1);
    job.run();
    assertEquals(Optional.of(MIN_NODES + loadDelta), newSize);
  }

  @Test
  public void testWeResizeIfStorageConstraintsAreNotMet() throws IOException {
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.90);
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, MIN_NODES + 1);
    SemanticMetricRegistry registry = new SemanticMetricRegistry();
    job =
        new AutoscaleJob(
            bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, Instant::now);
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.1);
    job.run();
    List<MetricId> metric =
        registry
            .getMeters()
            .keySet()
            .stream()
            .filter(meter -> meter.getTags().containsValue("overridden-desired-node-count"))
            .collect(Collectors.toList());

    assertEquals(1, metric.size());
    Map<String, String> tags = metric.get(0).getTags();
    assertEquals("storage-constraint", tags.get("reason"));
    assertEquals("9", tags.get("target-nodes"));
    assertEquals("5", tags.get("desired-nodes"));
    assertEquals(String.valueOf(MIN_NODES), tags.get("min-nodes"));
    assertEquals(String.valueOf(MAX_NODES), tags.get("max-nodes"));
    assertEquals(projectId, tags.get("project-id"));
    assertEquals(clusterId, tags.get("cluster-id"));
    assertEquals(instanceId, tags.get("instance-id"));
    assertEquals(Optional.of(9), newSize);
  }

  @Test
  public void testWeDontResizeTooSoonEvenIfStorageConstraintsAreNotMet() throws IOException {
    AutoscaleJobTestMocks.setCurrentDiskUtilization(stackdriverClient, 0.90);
    BigtableCluster cluster =
        BigtableClusterBuilder.from(this.cluster).lastChange(Instant.now()).build();
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, MIN_NODES + 1);
    job =
        new AutoscaleJob(
            bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, Instant::now);
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.1);
    job.run();
    assertEquals(Optional.empty(), newSize);
  }
}
