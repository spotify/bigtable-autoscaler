package com.spotify.autoscaler;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.codahale.metrics.Gauge;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.PostgresDatabase;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import org.junit.Test;

public class ClusterStatsTest {

  @Test
  public void testNodeCountMetrics() {
    SemanticMetricRegistry registry = new SemanticMetricRegistry();
    ClusterStats clusterStats = new ClusterStats(registry, mock(PostgresDatabase.class));
    int loadDelta = 10;
    int currentNodes = 10;
    int minNodes = 3;
    int maxNodes = 30;
    BigtableCluster bigtableCluster =
        new BigtableClusterBuilder()
            .projectId("project")
            .instanceId("instance")
            .minNodes(minNodes)
            .loadDelta(loadDelta)
            .maxNodes(maxNodes)
            .clusterId("cluster")
            .build();
    clusterStats.setStats(bigtableCluster, currentNodes);
    assertMetric(registry, "node-count", currentNodes);
    assertMetric(registry, "max-node-count", maxNodes);
    assertMetric(registry, "min-node-count", minNodes + loadDelta);
  }

  private void assertMetric(SemanticMetricRegistry registry, String what, int expected) {
    MetricId metricId =
        registry
            .getMetrics()
            .keySet()
            .stream()
            .filter(m -> m.getTags().containsValue(what))
            .findAny()
            .get();
    Gauge gauge = registry.getGauges().get(metricId);
    assertEquals(expected, gauge.getValue());
  }
}
