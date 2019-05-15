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
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.util.BigtableUtil;
import com.spotify.autoscaler.util.ErrorCode;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
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
  private static final List<String> METRICS =
      Arrays.asList(
          new String[] {
            "node-count",
            "cpu-util",
            "last-check-time",
            "consecutive-failure-count",
            "storage-util",
            "cpu-target-ratio"
          });

  private final ScheduledExecutorService cleanupExecutor =
      new ScheduledThreadPoolExecutor(
          1,
          new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
              return new Thread(r, "Cluster-Metrics-Cleaner");
            }
          });

  public ClusterStats(final SemanticMetricRegistry registry, final Database db) {
    this.registry = registry;
    this.db = db;
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
    private int nodeCount;
    private double cpuUtil;
    private int consecutiveFailureCount;
    private double storageUtil;
    private Optional<ErrorCode> lastErrorCode;

    int getNodeCount() {
      return nodeCount;
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

    void setNodeCount(final int nodeCount) {
      this.nodeCount = nodeCount;
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
        final int nodeCount,
        final int consecutiveFailureCount,
        final Optional<ErrorCode> lastErrorCode) {
      this.nodeCount = nodeCount;
      this.cluster = cluster;
      this.consecutiveFailureCount = consecutiveFailureCount;
      this.lastErrorCode = lastErrorCode;
    }
  }

  public void setStats(BigtableCluster cluster, int count) {
    final ClusterData clusterData =
        registeredClusters.putIfAbsent(
            cluster.clusterName(),
            new ClusterData(
                cluster, count, cluster.consecutiveFailureCount(), cluster.errorCode()));

    if (clusterData == null) {
      // First time we saw this cluster, register a gauge
      this.registry.register(
          APP_PREFIX
              .tagged("what", "node-count")
              .tagged("project-id", cluster.projectId())
              .tagged("cluster-id", cluster.clusterId())
              .tagged("instance-id", cluster.instanceId()),
          (Gauge<Integer>) () -> registeredClusters.get(cluster.clusterName()).getNodeCount());

      this.registry.register(
          APP_PREFIX
              .tagged("what", "last-check-time")
              .tagged("project-id", cluster.projectId())
              .tagged("cluster-id", cluster.clusterId())
              .tagged("instance-id", cluster.instanceId()),
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
                      .getSeconds());

      for (ErrorCode code : ErrorCode.values()) {
        this.registry.register(
            APP_PREFIX
                .tagged("what", "consecutive-failure-count")
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

      this.registry.register(
          APP_PREFIX
              .tagged("what", "cpu-target-ratio")
              .tagged("project-id", cluster.projectId())
              .tagged("cluster-id", cluster.clusterId())
              .tagged("instance-id", cluster.instanceId()),
          (Gauge<Double>)
              () -> {
                final ClusterData data = registeredClusters.get(cluster.clusterName());
                return data.getCpuUtil() / cluster.cpuTarget();
              });
    } else {
      clusterData.setNodeCount(count);
      clusterData.setConsecutiveFailureCount(cluster.consecutiveFailureCount());
      clusterData.setLastErrorCode(cluster.errorCode());
    }
  }

  public void setLoad(BigtableCluster cluster, double load, MetricType type) {
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

  enum MetricType {
    CPU("cpu-util"),
    STORAGE("storage-util");

    private String tag;

    MetricType(final String tag) {
      this.tag = tag;
    }
  }
}
