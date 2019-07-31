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

import com.codahale.metrics.Gauge;
import com.spotify.autoscaler.Application;
import com.spotify.autoscaler.ErrorCode;
import com.spotify.autoscaler.LoggerContext;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.Database;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.sun.management.UnixOperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*Helper class containing methods to register and measure autoscaler metrics.*/
public class AutoscalerMetrics {

  public static final MetricId APP_PREFIX = MetricId.build("key", Application.SERVICE_NAME);
  private static final Logger LOG = LoggerFactory.getLogger(AutoscalerMetrics.class);

  private static final Duration CLEANUP_INTERVAL = Duration.ofMinutes(1);
  private final SemanticMetricRegistry registry;
  private final Map<String, ClusterData> registeredClusters = new ConcurrentHashMap<>();

  public AutoscalerMetrics(final SemanticMetricRegistry registry) {
    this.registry = registry;
  }

  public void registerClusterDataMetrics(
      final BigtableCluster cluster, final int currentNodes, final Database db) {
    final ClusterData clusterData =
        new ClusterDataBuilder()
            .cluster(cluster)
            .currentNodeCount(currentNodes)
            .minNodeCount(cluster.minNodes())
            .maxNodeCount(cluster.maxNodes())
            .effectiveMinNodeCount(cluster.effectiveMinNodes())
            .consecutiveFailureCount(cluster.consecutiveFailureCount())
            .lastErrorCode(cluster.errorCode())
            .build();

    if (registeredClusters.putIfAbsent(cluster.clusterName(), clusterData) == null) {
      // First time we saw this cluster, register a gauge
      for (final ClusterDataGauges metric : ClusterDataGauges.values()) {
        registry.register(
            baseMetric(cluster).tagged("what", metric.getTag()),
            metric.getMetricValue(registeredClusters, cluster.clusterName(), db));
      }

      for (final ErrorCode code : ErrorCode.values()) {
        registry.register(
            baseMetric(cluster)
                .tagged("what", ErrorGauges.CONSECUTIVE_FAILURE_COUNT.getTag())
                .tagged("latest-error-code", code.name()),
            ErrorGauges.CONSECUTIVE_FAILURE_COUNT.getMetricValue(
                registeredClusters, cluster.clusterName(), code));
      }
    } else {
      // update metrics
      registeredClusters.put(cluster.clusterName(), clusterData);
    }
  }

  public void scheduleCleanup(final Database database) {
    final ScheduledExecutorService cleanupExecutor =
        new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "Cluster-Metrics-Cleaner"));
    cleanupExecutor.scheduleAtFixedRate(
        () -> {
          try {
            LOG.info("Cleanup running");
            unregisterInactiveClustersMetrics(registry, database);
          } catch (final Throwable t) {
            LOG.error("Cleanup task failed", t);
          }
        },
        CLEANUP_INTERVAL.toMillis(),
        CLEANUP_INTERVAL.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  private void unregisterInactiveClustersMetrics(
      final SemanticMetricRegistry registry, final Database database) {
    final Set<String> bigtableClusters = database.getActiveClusterKeys();
    for (final Map.Entry<String, ClusterData> entry : registeredClusters.entrySet()) {
      if (!bigtableClusters.contains(entry.getKey())) {
        registeredClusters.remove(entry.getKey());
        final BigtableCluster cluster = entry.getValue().cluster();
        LoggerContext.pushContext(cluster);
        registry.removeMatching(
            (name, m) -> {
              final Map<String, String> tags = name.getTags();
              return tags.getOrDefault("project-id", "").equals(cluster.projectId())
                  && tags.getOrDefault("instance-id", "").equals(cluster.instanceId())
                  && tags.getOrDefault("cluster-id", "").equals(cluster.clusterId())
                  && getAllMetrics().contains(tags.getOrDefault("what", ""));
            });

        LOG.info("Metrics unregistered");
        LoggerContext.clearContext();
      }
    }
  }

  public void registerClusterLoadMetrics(
      final BigtableCluster cluster, final double load, final ClusterLoadGauges type) {
    if (registeredClusters.get(cluster.clusterName()) == null) {
      return;
    }

    final ClusterDataBuilder clusterDataBuilder =
        ClusterDataBuilder.from(registeredClusters.get(cluster.clusterName()));
    switch (type) {
      case CPU:
        clusterDataBuilder.cpuUtil(load);
        break;
      case STORAGE:
        clusterDataBuilder.storageUtil(load);
        break;
      default:
        throw new IllegalArgumentException(String.format("Undefined ClusterLoadGauges %s", type));
    }
    registeredClusters.put(cluster.clusterName(), clusterDataBuilder.build());

    final MetricId metricId = baseMetric(cluster).tagged("what", type.getTag());
    if (!registry.getGauges().containsKey(metricId)) {
      registry.register(metricId, type.getMetricValue(registeredClusters, cluster.clusterName()));
    }
  }

  private MetricId baseMetric(final BigtableCluster cluster) {
    return APP_PREFIX
        .tagged("project-id", cluster.projectId())
        .tagged("cluster-id", cluster.clusterId())
        .tagged("instance-id", cluster.instanceId());
  }

  private static List<String> getAllMetrics() {
    final List<String> metrics = new ArrayList<>();
    Arrays.stream(ClusterDataGauges.values()).map(ClusterDataGauges::getTag).forEach(metrics::add);
    Arrays.stream(ClusterLoadGauges.values()).map(ClusterLoadGauges::getTag).forEach(metrics::add);
    Arrays.stream(ErrorGauges.values()).map(ErrorGauges::getTag).forEach(metrics::add);
    return metrics;
  }

  public void markStorageConstraint(
      final BigtableCluster cluster, final int desiredNodes, final int targetNodes) {
    registry
        .meter(
            constraintMetric(cluster, desiredNodes, targetNodes)
                .tagged("reason", "storage-constraint"))
        .mark();
  }

  public void markSizeConstraint(
      final int desiredNodes, final int finalNodes, final BigtableCluster cluster) {
    final MetricId metric = constraintMetric(cluster, desiredNodes, finalNodes);

    if (cluster.minNodes() > desiredNodes) {
      registry.meter(metric.tagged("reason", "min-nodes-constraint")).mark();
    }

    if (cluster.effectiveMinNodes() > desiredNodes) {
      registry.meter(metric.tagged("reason", "effective-min-nodes-constraint")).mark();
    }

    if (cluster.maxNodes() < desiredNodes) {
      registry.meter(metric.tagged("reason", "max-nodes-constraint")).mark();
    }
  }

  private MetricId constraintMetric(
      final BigtableCluster cluster, final int desiredNodes, final int targetNodes) {
    return baseMetric(cluster)
        .tagged("what", "overridden-desired-node-count")
        .tagged("desired-nodes", String.valueOf(desiredNodes))
        .tagged("min-nodes", String.valueOf(cluster.effectiveMinNodes()))
        .tagged("target-nodes", String.valueOf(targetNodes))
        .tagged("max-nodes", String.valueOf(cluster.maxNodes()));
  }

  public void markClusterCheck() {
    registry.meter(APP_PREFIX.tagged("what", "clusters-checked")).mark();
  }

  public void markCallToGetSize() {
    registry.meter(APP_PREFIX.tagged("what", "call-to-get-size")).mark();
  }

  public void markCallToSetSize() {
    registry.meter(APP_PREFIX.tagged("what", "call-to-set-size")).mark();
  }

  public void markClusterChanged() {
    registry.meter(APP_PREFIX.tagged("what", "clusters-changed")).mark();
  }

  public void markSetSizeError() {
    registry.meter(APP_PREFIX.tagged("what", "set-size-transport-error")).mark();
  }

  public void markHeartBeat() {
    registry.meter(APP_PREFIX.tagged("what", "autoscale-heartbeat")).mark();
  }

  public void registerOpenDatabaseConnections(final Database database) {
    registry.register(
        APP_PREFIX.tagged("what", "open-db-connections"),
        (Gauge<Integer>) database::getTotalConnections);
  }

  public void registerActiveClusters(final Database database) {
    registry.register(
        APP_PREFIX.tagged("what", "enabled-clusters"),
        (Gauge<Long>)
            () -> database.getBigtableClusters().stream().filter(BigtableCluster::enabled).count());

    registry.register(
        APP_PREFIX.tagged("what", "disabled-clusters"),
        (Gauge<Long>)
            () -> database.getBigtableClusters().stream().filter(p -> !p.enabled()).count());
  }

  public void registerOpenFileDescriptors() {
    registry.register(
        APP_PREFIX.tagged("what", "open-file-descriptors"),
        (Gauge<Long>)
            () ->
                ((UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                    .getOpenFileDescriptorCount());
  }

  public void registerDailyResizeCount(final Database database) {
    registry.register(
        APP_PREFIX.tagged("what", "daily-resize-count"),
        (Gauge<Long>) database::getDailyResizeCount);
  }

  public void registerFailureCount(final Database database) {
    for (final ErrorCode code : ErrorCode.values()) {
      registry.register(
          APP_PREFIX.tagged("what", "failing-cluster-count").tagged("error-code", code.name()),
          (Gauge<Long>)
              () ->
                  database
                      .getBigtableClusters()
                      .stream()
                      .filter(BigtableCluster::enabled)
                      .filter(p -> p.errorCode().orElse(ErrorCode.OK) == code)
                      .filter(p -> p.consecutiveFailureCount() > 0)
                      .count());
    }
  }
}
