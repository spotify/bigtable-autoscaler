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

import static com.spotify.autoscaler.Main.APP_PREFIX;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.metric.BigtableMetric;
import com.spotify.autoscaler.util.BigtableUtil;
import com.spotify.autoscaler.util.ErrorCode;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// DONTLIKEIT
// rename to BigtableMetricReporter? And move it to the metric package
// look at PR https://github.com/spotify/bigtable-autoscaler/pull/63/files
public class ClusterStats {

  private static final Logger logger = LoggerFactory.getLogger(ClusterStats.class);

  private static final Duration CLEANUP_INTERVAL = Duration.ofMinutes(1);
  private final SemanticMetricRegistry registry;
  private final Database db;
  private final Map<String, ClusterData> registeredClusters =
      new ConcurrentHashMap<String, ClusterData>();
  private static final List<String> METRICS = BigtableMetric.getAllMetrics();

  ClusterStats(final SemanticMetricRegistry registry, final Database db) {
    this.registry = registry;
    this.db = db;
    final ScheduledExecutorService cleanupExecutor =
        new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "Cluster-Metrics-Cleaner"));
    cleanupExecutor.scheduleAtFixedRate(
        () -> {
          try {
            logger.info("Cleanup running");
            unregisterInactiveClustersMetrics(registry, db);
          } catch (final Throwable t) {
            logger.error("Cleanup task failed", t);
          }
        },
        CLEANUP_INTERVAL.toMillis(),
        CLEANUP_INTERVAL.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  private void unregisterInactiveClustersMetrics(
      final SemanticMetricRegistry registry, final Database db) {
    final Set<String> bigtableClusters = db.getActiveClusterKeys();
    for (final Map.Entry<String, ClusterData> entry : registeredClusters.entrySet()) {
      if (!bigtableClusters.contains(entry.getKey())) {
        registeredClusters.remove(entry.getKey());
        final BigtableCluster cluster = entry.getValue().getCluster();
        BigtableUtil.pushContext(cluster);
        registry.removeMatching(
            (name, m) -> {
              final Map<String, String> tags = name.getTags();
              return tags.getOrDefault("project-id", "").equals(cluster.projectId())
                     && tags.getOrDefault("instance-id", "").equals(cluster.instanceId())
                     && tags.getOrDefault("cluster-id", "").equals(cluster.clusterId())
                     && METRICS.contains(tags.getOrDefault("what", ""));
            });

        logger.info("Metrics unregistered");
        BigtableUtil.clearContext();
      }
    }
  }

  void setStats(final BigtableCluster cluster, final int currentNodes) {
    final ClusterData clusterData =
        registeredClusters.putIfAbsent(
            cluster.clusterName(),
            new ClusterData(
                cluster, currentNodes, cluster.consecutiveFailureCount(), cluster.errorCode()));

    if (clusterData == null) {
      // First time we saw this cluster, register a gauge

      for (final BigtableMetric.Metrics metric : BigtableMetric.Metrics.values()) {
        final Gauge metricValue =
            metric.getMetricValue(registeredClusters.get(cluster.clusterName()), db);
        registerMetric(metric.tag, cluster, metricValue);
      }

      for (final ErrorCode code : ErrorCode.values()) {
        this.registry.register(
            APP_PREFIX
                .tagged("what", BigtableMetric.ErrorCode.CONSECUTIVE_FAILURE_COUNT.tag)
                .tagged("project-id", cluster.projectId())
                .tagged("cluster-id", cluster.clusterId())
                .tagged("instance-id", cluster.instanceId())
                .tagged("latest-error-code", code.name()),
            (Gauge<Integer>)
                () -> {
                  final ClusterData c = registeredClusters.get(cluster.clusterName());
                  return c.getLastErrorCode().orElse(ErrorCode.OK) == code
                         ? c.getConsecutiveFailureCount()
                         : 0;
                });
      }
    } else {
      // DONTLIKEIT
      // why we do this? We should create a new instance
      clusterData.setCurrentNodeCount(currentNodes);
      clusterData.setConsecutiveFailureCount(cluster.consecutiveFailureCount());
      clusterData.setLastErrorCode(cluster.errorCode());
      clusterData.setMinNodeCount(cluster.minNodes());
      clusterData.setMaxNodeCount(cluster.maxNodes());
      clusterData.setEffectiveMinNodeCount(cluster.effectiveMinNodes());
      clusterData.setCluster(cluster);
    }
  }

  // DONTLIKEIT
  // listen to Idea
  public <T extends Metric> T registerMetric(final String what, final BigtableCluster cluster, final T metric) {
    return this.registry.register(
        APP_PREFIX
            .tagged("what", what)
            .tagged("project-id", cluster.projectId())
            .tagged("cluster-id", cluster.clusterId())
            .tagged("instance-id", cluster.instanceId()),
        metric);
  }

  void setLoad(final BigtableCluster cluster, final double load, final BigtableMetric.LoadMetricType type) {
    if (registeredClusters.get(cluster.clusterName()) == null) {
      return;
    }
    final ClusterData clusterData = registeredClusters.get(cluster.clusterName());
    final Callable<Double> lambda;
    switch (type) {
      case CPU:
        clusterData.setCpuUtil(load);
        lambda = clusterData::getCpuUtil;
        break;
      case STORAGE:
        clusterData.setStorageUtil(load);
        lambda = clusterData::getStorageUtil;
        break;
      default:
        throw new IllegalArgumentException(String.format("Undefined LoadMetricType %s", type));
    }

    final MetricId key =
        APP_PREFIX
            .tagged("what", type.tag)
            .tagged("project-id", cluster.projectId())
            .tagged("cluster-id", cluster.clusterId())
            .tagged("instance-id", cluster.instanceId());

    if (!registry.getGauges().containsKey(key)) {
      this.registry.register(
          key,
          (Gauge<Double>)
              () -> {
                try {
                  return lambda.call();
                } catch (final Exception e) {
                  logger.error("Couldn't get metric", e);
                  return 0.0;
                }
              });
    }
  }
}
