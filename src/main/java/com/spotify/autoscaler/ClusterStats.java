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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterStats {

  private static final Logger logger = LoggerFactory.getLogger(ClusterStats.class);

  private static final Duration CLEANUP_INTERVAL = Duration.ofMinutes(1);
  private SemanticMetricRegistry registry;
  private Database db;
  private Map<String, ClusterData> registeredClusters =
      new ConcurrentHashMap<String, ClusterData>();
  private static final List<String> METRICS = BigtableMetric.getAllMetrics();

  ClusterStats(final SemanticMetricRegistry registry, final Database db) {
    this.registry = registry;
    this.db = db;
    ScheduledExecutorService cleanupExecutor =
        new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "Cluster-Metrics-Cleaner"));
    cleanupExecutor.scheduleAtFixedRate(
        () -> {
          try {
            logger.info("Cleanup running");
            unregisterInactiveClustersMetrics(registry, db);
          } catch (Throwable t) {
            logger.error("Cleanup task failed", t);
          }
        },
        CLEANUP_INTERVAL.toMillis(),
        CLEANUP_INTERVAL.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  private void unregisterInactiveClustersMetrics(
      final SemanticMetricRegistry registry, final Database db) {
    Set<String> bigtableClusters = db.getActiveClusterKeys();
    for (Map.Entry<String, ClusterData> entry : registeredClusters.entrySet()) {
      if (!bigtableClusters.contains(entry.getKey())) {
        registeredClusters.remove(entry.getKey());
        BigtableCluster cluster = entry.getValue().getCluster();
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

  private static class ClusterData {

    private BigtableCluster cluster;
    private int currentNodeCount;
    private int minNodeCount;
    private int maxNodeCount;
    private int effectiveMinNodeCount;
    private double cpuUtil;
    private int consecutiveFailureCount;
    private double storageUtil;
    private Optional<ErrorCode> lastErrorCode;

    int getCurrentNodeCount() {
      return currentNodeCount;
    }

    int getConsecutiveFailureCount() {
      return consecutiveFailureCount;
    }

    double getCpuUtil() {
      return cpuUtil;
    }

    Optional<ErrorCode> getLastErrorCode() {
      return lastErrorCode;
    }

    void setCurrentNodeCount(final int currentNodeCount) {
      this.currentNodeCount = currentNodeCount;
    }

    void setConsecutiveFailureCount(final int consecutiveFailureCount) {
      this.consecutiveFailureCount = consecutiveFailureCount;
    }

    void setLastErrorCode(final Optional<ErrorCode> lastErrorCode) {
      this.lastErrorCode = lastErrorCode;
    }

    double getStorageUtil() {
      return storageUtil;
    }

    void setStorageUtil(double storageUtil) {
      this.storageUtil = storageUtil;
    }

    void setCpuUtil(double cpuUtil) {
      this.cpuUtil = cpuUtil;
    }

    BigtableCluster getCluster() {
      return cluster;
    }

    private ClusterData(
        final BigtableCluster cluster,
        final int currentNodeCount,
        final int consecutiveFailureCount,
        final Optional<ErrorCode> lastErrorCode) {
      this.currentNodeCount = currentNodeCount;
      this.minNodeCount = cluster.minNodes();
      this.maxNodeCount = cluster.maxNodes();
      this.effectiveMinNodeCount = cluster.effectiveMinNodes();
      this.cluster = cluster;
      this.consecutiveFailureCount = consecutiveFailureCount;
      this.lastErrorCode = lastErrorCode;
    }

    int getMaxNodeCount() {
      return maxNodeCount;
    }

    void setMaxNodeCount(int maxNodeCount) {
      this.maxNodeCount = maxNodeCount;
    }

    int getMinNodeCount() {
      return minNodeCount;
    }

    void setMinNodeCount(int minNodeCount) {
      this.minNodeCount = minNodeCount;
    }

    public int getEffectiveMinNodeCount() {
      return effectiveMinNodeCount;
    }

    public void setEffectiveMinNodeCount(int effectiveMinNodeCount) {
      this.effectiveMinNodeCount = effectiveMinNodeCount;
    }
  }

  void setStats(BigtableCluster cluster, int currentNodes) {
    final ClusterData clusterData =
        registeredClusters.putIfAbsent(
            cluster.clusterName(),
            new ClusterData(
                cluster, currentNodes, cluster.consecutiveFailureCount(), cluster.errorCode()));

    if (clusterData == null) {
      // First time we saw this cluster, register a gauge

      for (BigtableMetric.Metrics metric : BigtableMetric.Metrics.values()) {
        Gauge metricValue = null;
        switch (metric) {
          case NODE_COUNT:
            metricValue =
                (Gauge<Integer>)
                    () -> registeredClusters.get(cluster.clusterName()).getCurrentNodeCount();
            break;
          case MIN_NODE_COUNT:
            metricValue =
                (Gauge<Integer>)
                    () -> registeredClusters.get(cluster.clusterName()).getMinNodeCount();
            break;
          case MAX_NODE_COUNT:
            metricValue =
                (Gauge<Integer>)
                    () -> registeredClusters.get(cluster.clusterName()).getMaxNodeCount();
            break;
          case EFFECTIVE_MIN_NODE_COUNT:
            metricValue =
                (Gauge<Integer>)
                    () -> registeredClusters.get(cluster.clusterName()).getEffectiveMinNodeCount();
            break;
          case LAST_CHECK_TIME:
            metricValue =
                (Gauge<Long>)
                    () ->
                        db.getBigtableCluster(
                                cluster.projectId(), cluster.instanceId(), cluster.clusterId())
                            .flatMap(
                                p ->
                                    Optional.of(
                                        Duration.between(
                                            p.lastCheck().orElse(Instant.EPOCH), Instant.now())))
                            .get()
                            .getSeconds();
            break;
          case CPU_TARGET_RATIO:
            metricValue =
                (Gauge<Double>)
                    () -> {
                      final ClusterData data = registeredClusters.get(cluster.clusterName());
                      return data.getCpuUtil() / cluster.cpuTarget();
                    };
            break;
          default:
            logger.error(
                "The metric "
                    + metric.tag
                    + " is not registered correctly for the "
                    + "cluster "
                    + cluster.clusterName());
            break;
        }
        if (metricValue != null) {
          registerMetric(metric.tag, cluster, metricValue);
        }
      }

      for (ErrorCode code : ErrorCode.values()) {
        this.registry.register(
            APP_PREFIX
                .tagged("what", BigtableMetric.ErrorCode.CONSECUTIVE_FAILURE_COUNT.tag)
                .tagged("project-id", cluster.projectId())
                .tagged("cluster-id", cluster.clusterId())
                .tagged("instance-id", cluster.instanceId())
                .tagged("latest-error-code", code.name()),
            (Gauge<Integer>)
                () -> {
                  ClusterData c = registeredClusters.get(cluster.clusterName());
                  return c.getLastErrorCode().orElse(ErrorCode.OK) == code
                      ? c.getConsecutiveFailureCount()
                      : 0;
                });
      }
    } else {
      clusterData.setCurrentNodeCount(currentNodes);
      clusterData.setConsecutiveFailureCount(cluster.consecutiveFailureCount());
      clusterData.setLastErrorCode(cluster.errorCode());
      clusterData.setMinNodeCount(cluster.minNodes());
      clusterData.setMaxNodeCount(cluster.maxNodes());
      clusterData.setEffectiveMinNodeCount(cluster.effectiveMinNodes());
    }
  }

  public <T extends Metric> T registerMetric(String what, BigtableCluster cluster, T metric) {
    System.out.println("what = " + what);
    System.out.println("cluster = " + cluster.clusterName());
    System.out.println("metric = " + metric.toString());
    return this.registry.register(
        APP_PREFIX
            .tagged("what", what)
            .tagged("project-id", cluster.projectId())
            .tagged("cluster-id", cluster.clusterId())
            .tagged("instance-id", cluster.instanceId()),
        metric);
  }

  void setLoad(BigtableCluster cluster, double load, BigtableMetric.MetricType type) {
    if (registeredClusters.get(cluster.clusterName()) == null) {
      return;
    }
    ClusterData clusterData = registeredClusters.get(cluster.clusterName());
    Callable<Double> lambda;
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
        throw new IllegalArgumentException(String.format("Undefined MetricType %s", type));
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
                } catch (Exception e) {
                  logger.error("Couldn't get metric", e);
                  return 0.0;
                }
              });
    }
  }
}
