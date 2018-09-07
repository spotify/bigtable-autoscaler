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

package com.spotify.autoscaler;

import static com.spotify.autoscaler.Main.APP_PREFIX;

import com.codahale.metrics.Gauge;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.util.BigtableUtil;
import com.spotify.metrics.core.SemanticMetricRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterStats {

  private static final Logger logger = LoggerFactory.getLogger(ClusterStats.class);

  private SemanticMetricRegistry registry;
  private Database db;
  private Map<String, ClusterData> nodes = new ConcurrentHashMap<String, ClusterData>();
  private final ScheduledExecutorService cleanupExecutor = new ScheduledThreadPoolExecutor(1);
  private static final Duration CLEANUP_INTERVAL = Duration.ofMinutes(1);

  public ClusterStats(final SemanticMetricRegistry registry, final Database db) {
    this.registry = registry;
    this.db = db;
    cleanupExecutor.scheduleWithFixedDelay(() -> {
          unregisterInactiveClustersMetrics(registry, db);

        }, CLEANUP_INTERVAL.toMillis(),
        CLEANUP_INTERVAL.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  private void unregisterInactiveClustersMetrics(final SemanticMetricRegistry registry, final Database db) {
    Set<String> bigtableClusters = db.getActiveClusterKeys();
    for (Map.Entry<String, ClusterData> entry : nodes.entrySet()) {
      if (!bigtableClusters.contains(entry.getKey())) {
        nodes.remove(entry.getKey());
        BigtableCluster cluster = entry.getValue().getCluster();
        BigtableUtil.pushContext(cluster);
        registry.remove(
            APP_PREFIX.tagged("what", "node-count").tagged("project-id", cluster.projectId()).tagged
                ("cluster-id", cluster.clusterId()).tagged("instance-id", cluster.instanceId()));
        registry.remove(
            APP_PREFIX.tagged("what", "cpu-util").tagged("project-id", cluster.projectId()).tagged
                ("cluster-id", cluster.clusterId()).tagged("instance-id", cluster.instanceId()));
        registry.remove(
            APP_PREFIX.tagged("what", "last-check-time").tagged("project-id", cluster.projectId()).tagged
                ("cluster-id", cluster.clusterId()).tagged("instance-id", cluster.instanceId()));

        logger.info("Metrics unregistered");
        BigtableUtil.clearContext();
      }
    }
  }

  private static class ClusterData {

    private BigtableCluster cluster;
    private int nodeCount;
    private double cpuUtil;

    int getNodeCount() {
      return nodeCount;
    }

    double getCpuUtil() {
      return cpuUtil;
    }

    BigtableCluster getCluster() {
      return cluster;
    }

    private ClusterData(final BigtableCluster cluster, final int nodeCount, final double cpuUtil) {
      this.nodeCount = nodeCount;
      this.cpuUtil = cpuUtil;
      this.cluster = cluster;
    }
  }

  public void setNodeCount(BigtableCluster cluster, int count, double cpuUtil) {
    final ClusterData clusterData = new ClusterData(cluster, count, cpuUtil);
    if (nodes.get(cluster.clusterName()) == null) {
      // First time we saw this cluster, register a gauge
      this.registry.register(APP_PREFIX.tagged("what", "node-count").tagged("project-id", cluster.projectId()).tagged
              ("cluster-id", cluster.clusterId()).tagged("instance-id", cluster.instanceId()),
          (Gauge<Integer>) () -> nodes.get(cluster.clusterName()).getNodeCount());

      this.registry.register(APP_PREFIX.tagged("what", "cpu-util").tagged("project-id", cluster.projectId()).tagged
              ("cluster-id", cluster.clusterId()).tagged("instance-id", cluster.instanceId()),
          (Gauge<Double>) () -> nodes.get(cluster.clusterName()).getCpuUtil());

      this.registry.register(APP_PREFIX.tagged("what", "last-check-time").tagged("project-id", cluster.projectId())
              .tagged
                  ("cluster-id", cluster.clusterId()).tagged("instance-id", cluster.instanceId()),
          (Gauge<Long>) () -> db.getBigtableCluster(cluster.projectId(), cluster.instanceId(), cluster.clusterId())
              .flatMap(p -> Optional.of(Duration.between(p.lastCheck().orElse(Instant.EPOCH), Instant.now())))
              .get()
              .getSeconds());
    }
    nodes.put(cluster.clusterName(), clusterData);

  }
}
