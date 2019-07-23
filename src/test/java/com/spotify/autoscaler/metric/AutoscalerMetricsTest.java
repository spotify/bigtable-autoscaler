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

import com.codahale.metrics.Gauge;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.PostgresDatabase;
import com.spotify.autoscaler.util.ErrorCode;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import org.junit.Test;

public class AutoscalerMetricsTest {
  private final SemanticMetricRegistry registry = new SemanticMetricRegistry();

  @Test
  public void testClusterDataMetrics() {
    final AutoscalerMetrics autoscalerMetrics =
        new AutoscalerMetrics(registry, mock(PostgresDatabase.class));
    int minNodes = 10;
    int maxNodes = 200;
    int loadDelta = 0;
    int currentNodes = 20;
    autoscalerMetrics.registerClusterDataMetrics(
        new BigtableClusterBuilder()
            .projectId("project")
            .instanceId("instance")
            .minNodes(minNodes)
            .maxNodes(maxNodes)
            .loadDelta(loadDelta)
            .clusterId("cluster")
            .build(),
        currentNodes);
    assertMetric(registry, "node-count", currentNodes);
    assertMetric(registry, "max-node-count", maxNodes);
    assertMetric(registry, "min-node-count", minNodes);
    assertMetric(registry, "effective-min-node-count", minNodes + loadDelta);

    // verify changes are tracked in metrics properly
    autoscalerMetrics.registerClusterDataMetrics(
        new BigtableClusterBuilder()
            .projectId("project")
            .instanceId("instance")
            .minNodes(minNodes + 10)
            .maxNodes(maxNodes + 10)
            .loadDelta(loadDelta + 10)
            .clusterId("cluster")
            .build(),
        currentNodes + 10);
    assertMetric(registry, "node-count", currentNodes + 10);
    assertMetric(registry, "max-node-count", maxNodes + 10);
    assertMetric(registry, "min-node-count", minNodes + 10);
    assertMetric(registry, "effective-min-node-count", minNodes + loadDelta + 20);
  }

  @Test
  public void testClusterLoadMetrics() {
    final AutoscalerMetrics autoscalerMetrics =
        new AutoscalerMetrics(registry, mock(PostgresDatabase.class));
    BigtableCluster bigtableCluster =
        new BigtableClusterBuilder()
            .projectId("project")
            .instanceId("instance")
            .clusterId("cluster")
            .build();

    // Needs to be called before calling load Metrics
    autoscalerMetrics.registerClusterDataMetrics(bigtableCluster, 20);
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
    final AutoscalerMetrics autoscalerMetrics =
        new AutoscalerMetrics(registry, mock(PostgresDatabase.class));
    ErrorCode errorCode = ErrorCode.PROJECT_NOT_FOUND;
    BigtableCluster bigtableCluster =
        new BigtableClusterBuilder()
            .errorCode(errorCode)
            .projectId("project")
            .instanceId("instance")
            .clusterId("cluster")
            .consecutiveFailureCount(10)
            .build();
    autoscalerMetrics.registerClusterDataMetrics(bigtableCluster, 10);
    assertMetric(registry, errorCode.name(), 10);

    // verify changes are tracked in metrics properly
    autoscalerMetrics.registerClusterDataMetrics(
        BigtableClusterBuilder.from(bigtableCluster).consecutiveFailureCount(11).build(), 10);
    assertMetric(registry, errorCode.name(), 11);
  }

  private <T> void assertMetric(
      final SemanticMetricRegistry registry, final String what, final T expected) {
    final MetricId metricId =
        registry
            .getMetrics()
            .keySet()
            .stream()
            .filter(m -> m.getTags().containsValue(what))
            .findAny()
            .get();
    final Gauge gauge = registry.getGauges().get(metricId);
    assertEquals(expected, gauge.getValue());
  }
}
