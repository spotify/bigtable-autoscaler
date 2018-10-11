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
import com.spotify.metrics.core.SemanticMetricRegistry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class AutoscaleJobTest {

  @Mock
  BigtableSession bigtableSession;

  @Mock
  BigtableInstanceClient bigtableInstanceClient;

  @Mock
  StackdriverClient stackdriverClient;

  @Mock
  Database db;

  @Mock
  SemanticMetricRegistry registry;

  @Mock
  ClusterStats clusterStats;

  BigtableCluster cluster = new BigtableClusterBuilder()
      .projectId("project").instanceId("instance").clusterId("cluster")
      .cpuTarget(0.8).maxNodes(500).minNodes(5).overloadStep(100).build();
  int newSize;
  AutoscaleJob job;

  @Before
  public void setUp() throws IOException {
    initMocks(this);
    when(registry.meter(any())).thenReturn(new Meter());
    when(bigtableSession.getInstanceAdminClient()).thenReturn(bigtableInstanceClient);
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, 100);
    when(stackdriverClient.getDiskUtilization(any())).thenReturn(new Double(0.00000001));
    job = new AutoscaleJob(bigtableSession, stackdriverClient, this.cluster, db, registry, clusterStats, () -> Instant.now());
    when(bigtableInstanceClient.updateCluster(any()))
        .thenAnswer(
            invocationOnMock -> {
              newSize = ((Cluster) invocationOnMock.getArgument(0)).getServeNodes();
              AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, newSize);
              return null;
            });
  }

  @Test
  public void testSetup() {
  }

  @Test
  public void testResize() {
    // Test that we resize to correct target size
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.6);
    job.run();
    assertEquals(75, newSize);
  }

  @Test
  public void testUpperBound() {
    // Test that we don't go over maximum size
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, 480);
    job = new AutoscaleJob(bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, () -> Instant.now());
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.9);
    job.run();
    assertEquals(500, newSize);
  }

  @Test
  public void testLowerBound() {
    // Test that we don't go under minimum size
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, 6);
    job = new AutoscaleJob(bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, () -> Instant.now());
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.5);
    job.run();
    assertEquals(5, newSize);
  }

  @Test
  public void testHugeResizeOnOverload() {
    // To give the cluster a chance to settle in, don't resize too often
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.95);
    job.run();
    assertEquals(200, newSize);
  }

  @Test(expected = RuntimeException.class)
  public void testJobCantRunTwice() {
    job.run();
    job.run();
  }

  @Test
  public void testExponentialBackoffAfterConsecutiveFailures() {
    Instant now = Instant.now();

    BigtableCluster cluster = BigtableClusterBuilder.from(this.cluster)
        .consecutiveFailureCount(5) // 5 failures = 8 minutes
        .lastFailure(now)
        .build();

    job = new AutoscaleJob(bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, () -> now.plusSeconds(300));
    assertTrue(job.shouldExponentialBackoff());
    job = new AutoscaleJob(bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, () -> now.plusSeconds(1000));
    assertFalse(job.shouldExponentialBackoff());
  }

  @Test
  public void testNoExponentialBackoffAfterSuccess() {
    Instant now = Instant.now();

    BigtableCluster cluster = BigtableClusterBuilder.from(this.cluster)
        .consecutiveFailureCount(0) // last time succeeded
        .lastFailure(now)
        .build();

    job = new AutoscaleJob(bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, () -> now.plusSeconds(50));
    assertFalse(job.shouldExponentialBackoff());
  }

  @Test
  public void testThatWeDontReduceClusterSizeTooFast() {
    // Even if we're very over-provisioned, only reduce by 30 %
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.3);
    job.run();
    assertEquals(70, newSize);
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
  public void testWeResizeIfSizeConstraintsAreNotMet() {
    BigtableCluster cluster = BigtableClusterBuilder.from(this.cluster)
        .loadDelta(10)
        .lastChange(Instant.now())
        .build();
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, 5);
    job = new AutoscaleJob(bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, () -> Instant.now());
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.1);
    job.run();
    assertEquals(15, newSize);
  }

  @Test
  public void testWeResizeIfStorageConstraintsAreNotMet() {
    when(stackdriverClient.getDiskUtilization(any())).thenReturn(new Double(0.90));
    BigtableCluster cluster = BigtableClusterBuilder.from(this.cluster)
        .lastChange(Instant.now())
        .build();
    AutoscaleJobTestMocks.setCurrentSize(bigtableInstanceClient, 5);
    job = new AutoscaleJob(bigtableSession, stackdriverClient, cluster, db, registry, clusterStats, () -> Instant.now());
    AutoscaleJobTestMocks.setCurrentLoad(stackdriverClient, 0.1);
    job.run();
    assertEquals(7, newSize);
  }

  @Test
  public void testDecideFinalNodeCount1() {
    assertEquals(5,
        AutoscaleJob.decideFinalNodeCount(3, 4, 10, Optional.of(1), 5, false));
  }

  @Test
  public void testDecideFinalNodeCount2() {
    assertEquals(4,
        AutoscaleJob.decideFinalNodeCount(4, 4, 10, Optional.of(1), 5, false));
  }

  @Test
  public void testDecideFinalNodeCount3() {
    assertEquals(7,
        AutoscaleJob.decideFinalNodeCount(10, 4, 8, Optional.of(7), 6, false));
  }

  @Test
  public void testDecideFinalNodeCount4() {
    assertEquals(6,
        AutoscaleJob.decideFinalNodeCount(10, 4, 8, Optional.of(1), 6, false));
  }

  @Test
  public void testDecideFinalNodeCount5() {
    assertEquals(5,
        AutoscaleJob.decideFinalNodeCount(5, 4, 10, Optional.of(1), 5, true));
  }

  @Test
  public void testDecideFinalNodeCount6() {
    assertEquals(8,
        AutoscaleJob.decideFinalNodeCount(10, 3, 8, Optional.empty(), 3, true));
  }

  @Test
  public void testDecideFinalNodeCount7() {
    assertEquals(3,
        AutoscaleJob.decideFinalNodeCount(10, 3, 8, Optional.of(1), 3, true));
  }

  @Test
  public void testDecideFinalNodeCount8() {
    assertEquals(6,
        AutoscaleJob.decideFinalNodeCount(5, 3, 10, Optional.of(1), 6, true));
  }

  @Test
  public void testDecideFinalNodeCount9() {
    assertEquals(10,
        AutoscaleJob.decideFinalNodeCount(10, 3, 10, Optional.empty(), 3, true));
  }

  @Test
  public void testDecideFinalNodeCount10() {
    assertEquals(3,
        AutoscaleJob.decideFinalNodeCount(10, 3, 10, Optional.of(0), 3, true));
  }

  @Test
  public void testDiskConstraintOverridesCpuTargetedNodeCount() {
    assertEquals(115,
        AutoscaleJob.decideFinalNodeCount(100, 5, 500, Optional.of(115), 50, true));
  }

  @Test
  public void testDiskConstraintOverridesIfNotLoaded() {
    assertEquals(86,
        AutoscaleJob.decideFinalNodeCount(100, 5, 500, Optional.of(86), 50, true));
  }

  @Test
  public void testDiskConstraintDoesNotOverrideIfDesiredNodesAlreadyEnoughIfNotLoaded(){
    assertEquals(90,
        AutoscaleJob.decideFinalNodeCount(100, 5, 500, Optional.of(86), 90, true));
  }

  @Test
  public void testDiskConstraintDoesNotOverrideIfDesiredNodesAlreadyEnough(){
    assertEquals(120,
        AutoscaleJob.decideFinalNodeCount(100, 5, 500, Optional.of(115), 120, true));
  }
}
