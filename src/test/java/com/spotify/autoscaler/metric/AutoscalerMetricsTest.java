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

package com.spotify.autoscaler.metric;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Gauge;
import com.google.common.collect.ImmutableList;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.ErrorCode;
import com.spotify.autoscaler.db.PostgresDatabase;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import java.util.Collections;
import org.junit.Test;

public class AutoscalerMetricsTest {

  private final SemanticMetricRegistry registry = new SemanticMetricRegistry();

  private final PostgresDatabase db = mock(PostgresDatabase.class);
  private final BigtableClusterBuilder clusterBuilder =
      new BigtableClusterBuilder().projectId("project").instanceId("instance").clusterId("cluster");
  private final AutoscalerMetrics autoscalerMetrics = new AutoscalerMetrics(registry);

  @Test
  public void testClusterDataMetrics() {
    final int minNodes = 10;
    final int maxNodes = 200;
    final int minNodesOverride = 15;
    final int currentNodes = 20;
    final BigtableCluster bigtableCluster1 =
        clusterBuilder
            .minNodes(minNodes)
            .maxNodes(maxNodes)
            .minNodesOverride(minNodesOverride)
            .build();
    autoscalerMetrics.registerClusterDataMetrics(bigtableCluster1, currentNodes, db);
    assertMetric(registry, "node-count", currentNodes);
    assertMetric(registry, "max-node-count", maxNodes);
    assertMetric(registry, "min-node-count", minNodes);
    assertMetric(registry, "effective-min-node-count", minNodesOverride);

    // verify changes are tracked in metrics properly
    final BigtableCluster bigtableCluster2 =
        clusterBuilder
            .minNodes(minNodes + 10)
            .maxNodes(maxNodes + 10)
            .minNodesOverride(minNodesOverride)
            .clusterId("cluster")
            .build();
    autoscalerMetrics.registerClusterDataMetrics(bigtableCluster2, currentNodes + 10, db);
    assertMetric(registry, "node-count", currentNodes + 10);
    assertMetric(registry, "max-node-count", maxNodes + 10);
    assertMetric(registry, "min-node-count", minNodes + 10);
    assertMetric(registry, "effective-min-node-count", minNodes + 10);
  }

  @Test
  public void testClusterLoadMetrics() {
    final BigtableCluster bigtableCluster = clusterBuilder.build();

    // Needs to be called before calling load Metrics
    autoscalerMetrics.registerClusterDataMetrics(bigtableCluster, 20, db);
    autoscalerMetrics.registerClusterLoadMetrics(bigtableCluster, 60.0, ClusterLoadGauges.CPU);
    assertMetric(registry, "cpu-util", 60.0);

    autoscalerMetrics.registerClusterLoadMetrics(bigtableCluster, 30.0, ClusterLoadGauges.STORAGE);
    assertMetric(registry, "storage-util", 30.0);

    // verify changes are tracked in metrics properly
    autoscalerMetrics.registerClusterLoadMetrics(bigtableCluster, 50.0, ClusterLoadGauges.CPU);
    assertMetric(registry, "cpu-util", 50.0);

    autoscalerMetrics.registerClusterLoadMetrics(bigtableCluster, 30.1, ClusterLoadGauges.STORAGE);
    assertMetric(registry, "storage-util", 30.1);
  }

  @Test
  public void testErrorMetrics() {
    final ErrorCode errorCode = ErrorCode.PROJECT_NOT_FOUND;
    final BigtableCluster bigtableCluster =
        clusterBuilder.errorCode(errorCode).consecutiveFailureCount(10).build();
    autoscalerMetrics.registerClusterDataMetrics(bigtableCluster, 10, db);
    assertMetric(registry, errorCode.name(), 10);

    // verify changes are tracked in metrics properly
    autoscalerMetrics.registerClusterDataMetrics(
        BigtableClusterBuilder.from(bigtableCluster).consecutiveFailureCount(11).build(), 10, db);
    assertMetric(registry, errorCode.name(), 11);
  }

  @Test
  public void testDatabaseMetrics() {
    when(db.getTotalConnections()).thenReturn(10);
    autoscalerMetrics.registerOpenDatabaseConnections(db);

    assertMetric(registry, "open-db-connections", 10);

    // verify changes are tracked in metrics properly
    when(db.getTotalConnections()).thenReturn(20);
    assertMetric(registry, "open-db-connections", 20);
  }

  @Test
  public void testClusterStatus() {
    when(db.getBigtableClusters())
        .thenReturn(
            ImmutableList.of(
                clusterBuilder.enabled(true).build(), clusterBuilder.enabled(false).build()));

    autoscalerMetrics.registerActiveClusters(db);

    assertMetric(registry, "enabled-clusters", 1L);
    assertMetric(registry, "disabled-clusters", 1L);

    when(db.getBigtableClusters()).thenReturn(Collections.emptyList());

    assertMetric(registry, "enabled-clusters", 0L);
    assertMetric(registry, "disabled-clusters", 0L);
  }

  @Test
  public void testDailyResizeCount() {
    when(db.getDailyResizeCount()).thenReturn(100L);
    autoscalerMetrics.registerDailyResizeCount(db);

    assertMetric(registry, "daily-resize-count", 100L);
    when(db.getDailyResizeCount()).thenReturn(110L);
    assertMetric(registry, "daily-resize-count", 110L);
  }

  private <T> void assertMetric(
      final SemanticMetricRegistry registry, final String what, final T expected) {
    final MetricId metricId =
        registry.getMetrics().keySet().stream()
            .filter(m -> m.getTags().containsValue(what))
            .findAny()
            .get();
    final Gauge gauge = registry.getGauges().get(metricId);
    assertEquals(expected, gauge.getValue());
  }
}
