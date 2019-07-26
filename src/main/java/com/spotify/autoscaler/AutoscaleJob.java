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

import static com.google.api.client.util.Preconditions.checkNotNull;

import com.google.bigtable.admin.v2.Cluster;
import com.google.bigtable.admin.v2.GetClusterRequest;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.common.annotations.VisibleForTesting;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.ClusterResizeLog;
import com.spotify.autoscaler.db.ClusterResizeLogBuilder;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.metric.AutoscalerMetrics;
import com.spotify.autoscaler.metric.ClusterLoadGauges;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoscaleJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(AutoscaleJob.class);

  private final StackdriverClient stackdriverClient;
  private final Database database;
  private final AutoscalerMetrics autoscalerMetrics;

  public static final Duration CHECK_INTERVAL = Duration.ofSeconds(30);

  // CPU related constants
  private static final double MAX_REDUCTION_RATIO = 0.7;
  private static final double CPU_OVERLOAD_THRESHOLD = 0.9;
  private static final Duration MAX_SAMPLE_INTERVAL = Duration.ofHours(1);

  // Google recommends keeping the disk utilization around 70% to accommodate sudden spikes
  // see <<Storage utilization per node>> section for details,
  // https://cloud.google.com/bigtable/quotas
  private static final double MAX_DISK_UTILIZATION_PERCENTAGE = 0.7d;

  // Time related constants
  private static final Duration AFTER_CHANGE_SAMPLE_BUFFER_TIME = Duration.ofMinutes(5);
  private static final Duration RESIZE_SETTLE_TIME = Duration.ofMinutes(5);
  private static final Duration MINIMUM_CHANGE_INTERVAL =
      RESIZE_SETTLE_TIME.plus(AFTER_CHANGE_SAMPLE_BUFFER_TIME);
  private static final Duration MAX_FAILURE_BACKOFF_INTERVAL = Duration.ofHours(4);
  // This means that a 2 % capacity increase can only happen every second hour,
  // but a twenty percent change can happen as often as every twelve minutes.
  private static final double MINIMUM_UPSCALE_WEIGHT = 14400;
  // This means that a 2 % capacity decrease can only happen every eight hours,
  // but a twenty percent change can happen as often as every 48 minutes.
  private static final double MINIMUM_DOWNSCALE_WEIGHT = 57600;

  public AutoscaleJob(
      final StackdriverClient stackdriverClient,
      final Database database,
      final AutoscalerMetrics autoscalerMetrics) {
    this.stackdriverClient = checkNotNull(stackdriverClient);
    this.autoscalerMetrics = checkNotNull(autoscalerMetrics);
    this.database = checkNotNull(database);
  }

  private int getSize(final Cluster clusterInfo) {
    autoscalerMetrics.markCallToGetSize();
    return clusterInfo.getServeNodes();
  }

  private void setSize(
      final BigtableSession bigtableSession,
      final String clusterName,
      final ClusterResizeLogBuilder clusterResizeLogBuilder,
      final int newSize) {
    final Cluster newSizeCluster =
        Cluster.newBuilder().setName(clusterName).setServeNodes(newSize).build();

    // two separate metrics here;
    // clusters-changed would only reflect the ones that are successfully changed, while
    // call-to-set-size would reflect the number of API calls, to monitor if we are reaching the
    // daily limit
    try {
      autoscalerMetrics.markCallToSetSize();
      clusterResizeLogBuilder.targetNodes(newSize);
      bigtableSession.getInstanceAdminClient().updateCluster(newSizeCluster);
      clusterResizeLogBuilder.success(true);
      autoscalerMetrics.markClusterChanged();
    } catch (final IOException e) {
      LOGGER.error("Failed to set cluster size", e);
      clusterResizeLogBuilder.errorMessage(Optional.of(e.toString()));
      clusterResizeLogBuilder.success(false);
      autoscalerMetrics.markSetSizeError();
    } catch (final Throwable t) {
      clusterResizeLogBuilder.errorMessage(Optional.of(t.toString()));
      clusterResizeLogBuilder.success(false);
      throw t;
    } finally {
      database.logResize(clusterResizeLogBuilder.build());
    }
  }

  private Duration getDurationSinceLastChange(
      final BigtableCluster cluster, final Supplier<Instant> timeSupplier) {
    // the database always stores UTC time
    // so remember to use UTC time as well when comparing with the database
    // for example Instant.now() returns UTC time
    final Instant now = timeSupplier.get();
    final Instant lastChange = cluster.lastChange().orElse(Instant.EPOCH);
    return Duration.between(lastChange, now);
  }

  private Duration getSamplingDuration(
      final BigtableCluster cluster, final Supplier<Instant> timeSupplier) {
    final Duration timeSinceLastChange = getDurationSinceLastChange(cluster, timeSupplier);
    return computeSamplingDuration(timeSinceLastChange);
  }

  @VisibleForTesting
  Duration computeSamplingDuration(final Duration timeSinceLastChange) {
    final Duration reducedTimeSinceLastChange =
        timeSinceLastChange.minus(AFTER_CHANGE_SAMPLE_BUFFER_TIME);
    return reducedTimeSinceLastChange.compareTo(MAX_SAMPLE_INTERVAL) <= 0
        ? reducedTimeSinceLastChange
        : MAX_SAMPLE_INTERVAL;
  }

  /*
   * If we haven't recently resized, also sample longer time intervals and choose the
   * highest load to avoid scaling down too much on transient load dips, while still
   * quickly scaling up on load increases. Basically, we want to scale up optimistically
   * but scale down pessimistically.
   */
  private double getCurrentCpu(final BigtableCluster cluster, final Duration samplingDuration) {
    return stackdriverClient.getCpuLoad(cluster, samplingDuration);
  }

  private int cpuStrategy(
      final BigtableCluster cluster,
      final ClusterResizeLogBuilder clusterResizeLogBuilder,
      final Duration samplingDuration,
      final int nodes) {
    double currentCpu = 0d;

    try {
      currentCpu = getCurrentCpu(cluster, samplingDuration);
    } finally {
      autoscalerMetrics.registerClusterLoadMetrics(cluster, currentCpu, ClusterLoadGauges.CPU);
    }

    LOGGER.info(
        "Running autoscale job. Nodes: {} (min={}, max={}, effectiveMinNodes={}), CPU: {} (target={})",
        nodes,
        cluster.minNodes(),
        cluster.maxNodes(),
        cluster.effectiveMinNodes(),
        currentCpu,
        cluster.cpuTarget());

    final double initialDesiredNodes = currentCpu * nodes / cluster.cpuTarget();

    final double desiredNodes;
    final boolean scaleDown = (initialDesiredNodes < nodes);
    final String path;
    if ((currentCpu > CPU_OVERLOAD_THRESHOLD) && cluster.overloadStep().isPresent()) {
      // If cluster is overloaded and the overloadStep is set, bump by that amount
      path = "overload";
      desiredNodes = nodes + cluster.overloadStep().get();
    } else if (scaleDown) {
      // Don't reduce node count too quickly to avoid overload from tablet shuffling
      path = "throttled change";
      desiredNodes = Math.max(initialDesiredNodes, MAX_REDUCTION_RATIO * nodes);
    } else {
      path = "normal";
      desiredNodes = initialDesiredNodes;
    }

    final int roundedDesiredNodes = (int) Math.ceil(desiredNodes);
    LOGGER.info(
        "Ideal node count: {}. Revised nodes: {}. Reason: {}.",
        initialDesiredNodes,
        desiredNodes,
        path);
    clusterResizeLogBuilder.cpuUtilization(currentCpu);
    clusterResizeLogBuilder.addResizeReason(" >>CPU strategy: " + path);
    return roundedDesiredNodes;
  }

  @VisibleForTesting
  boolean shouldExponentialBackoff(
      final BigtableCluster cluster, final Supplier<Instant> timeSupplier) {
    final Instant now = timeSupplier.get();

    if (cluster.lastFailure().isPresent() && cluster.consecutiveFailureCount() > 0) {
      // Last try resulted in a failure. Exponential backoff further tries.
      // After the first failure, the next attempt will be 1 minute after the failure, then 2, 4, 8
      // etc minutes
      // But never more than 4 hours.
      Duration nextTryDuration = MAX_FAILURE_BACKOFF_INTERVAL;
      if (cluster.consecutiveFailureCount() < 10) {
        // 9 consecutive failures would result in a duration ~4.3 hours,
        // so no need to calculate the exponential duration for anything >9
        nextTryDuration =
            CHECK_INTERVAL.multipliedBy((long) Math.pow(2, cluster.consecutiveFailureCount()));
        if (nextTryDuration.compareTo(MAX_FAILURE_BACKOFF_INTERVAL) > 0) {
          nextTryDuration = MAX_FAILURE_BACKOFF_INTERVAL;
        }
      }

      final Instant nextTry = cluster.lastFailure().get().plus(nextTryDuration);
      if (nextTry.isAfter(now)) {
        LOGGER.info(
            "Skipping autoscale check due to earlier failures; exponential backoff - next try at {}",
            nextTry);
        return true;
      }
    }

    return false;
  }

  private int storageConstraints(
      final BigtableCluster cluster,
      final ClusterResizeLogBuilder clusterResizeLogBuilder,
      final Duration samplingDuration,
      final int desiredNodes,
      final int currentNodes) {
    Double storageUtilization = 0.0;
    try {
      storageUtilization = stackdriverClient.getDiskUtilization(cluster, samplingDuration);
    } finally {
      autoscalerMetrics.registerClusterLoadMetrics(
          cluster, storageUtilization, ClusterLoadGauges.STORAGE);
    }
    if (storageUtilization <= 0.0) {
      return Math.max(currentNodes, desiredNodes);
    }
    final int minNodesRequiredForStorage =
        (int) Math.ceil(storageUtilization * currentNodes / MAX_DISK_UTILIZATION_PERCENTAGE);
    LOGGER.info(
        "Minimum nodes for storage: {}, currentUtilization: {}, current nodes: {}",
        minNodesRequiredForStorage,
        storageUtilization.toString(),
        currentNodes);
    clusterResizeLogBuilder.storageUtilization(storageUtilization);
    if (minNodesRequiredForStorage > desiredNodes) {
      autoscalerMetrics.markStorageConstraint(cluster, desiredNodes, minNodesRequiredForStorage);
      clusterResizeLogBuilder.addResizeReason(
          String.format(
              " >>Storage strategy: Target node count overridden(%d -> %d).",
              desiredNodes, minNodesRequiredForStorage));
    }
    return Math.max(minNodesRequiredForStorage, desiredNodes);
  }

  private int sizeConstraints(
      final BigtableCluster cluster,
      final ClusterResizeLogBuilder clusterResizeLogBuilder,
      final int desiredNodes) {
    // the desired size should be inside the autoscale boundaries
    final int finalNodes =
        Math.max(cluster.effectiveMinNodes(), Math.min(cluster.maxNodes(), desiredNodes));
    if (desiredNodes != finalNodes) {
      autoscalerMetrics.markSizeConstraint(desiredNodes, finalNodes, cluster);
      clusterResizeLogBuilder.addResizeReason(
          String.format(
              " >>Size strategy: Target count overridden(%d -> %d)", desiredNodes, finalNodes));
    }
    return finalNodes;
  }

  private boolean isTooEarlyToFetchMetrics(
      final BigtableCluster cluster, final Supplier<Instant> timeSupplier) {
    final Duration timeSinceLastChange = getDurationSinceLastChange(cluster, timeSupplier);
    return timeSinceLastChange.minus(MINIMUM_CHANGE_INTERVAL).isNegative();
  }

  // Implements a strategy to avoid autoscaling too often
  private int frequencyConstraints(
      final BigtableCluster cluster,
      final Supplier<Instant> timeSupplier,
      final int nodes,
      final int currentNodes) {
    final Duration timeSinceLastChange = getDurationSinceLastChange(cluster, timeSupplier);
    int desiredNodes = nodes;
    // It's OK to do large changes often if needed, but only do small changes very rarely to avoid
    // too much oscillation
    final double changeWeight =
        100.0
            * Math.abs(1.0 - (double) desiredNodes / currentNodes)
            * timeSinceLastChange.getSeconds();
    final boolean scaleDown = (desiredNodes < currentNodes);
    String path = "normal";

    if (scaleDown && (changeWeight < MINIMUM_DOWNSCALE_WEIGHT)) {
      // Avoid downscaling too frequently
      path = "downscale too small/frequent";
      desiredNodes = currentNodes;
    } else if (!scaleDown && (changeWeight < MINIMUM_UPSCALE_WEIGHT)) {
      // Avoid upscaling too frequently
      path = "upscale too small/frequent";
      desiredNodes = currentNodes;
    }

    LOGGER.info("Ideal node count: {}. Revised nodes: {}. Reason: {}.", nodes, desiredNodes, path);
    return desiredNodes;
  }

  void run(
      final BigtableCluster cluster,
      final BigtableSession session,
      final Supplier<Instant> timeSupplier)
      throws IOException {

    if (shouldExponentialBackoff(cluster, timeSupplier)) {
      LOGGER.info("Exponential backoff");
      return;
    }
    final BigtableInstanceClient instanceAdminClient = session.getInstanceAdminClient();
    final Cluster clusterInfo =
        instanceAdminClient.getCluster(
            GetClusterRequest.newBuilder().setName(cluster.clusterName()).build());
    final int currentNodes = getSize(clusterInfo);
    final ClusterResizeLogBuilder clusterResizeLogBuilder = ClusterResizeLog.builder(cluster);
    clusterResizeLogBuilder.currentNodes(currentNodes);
    autoscalerMetrics.registerClusterDataMetrics(cluster, currentNodes, database);
    autoscalerMetrics.markClusterCheck();

    if (isTooEarlyToFetchMetrics(cluster, timeSupplier)) {
      final int newNodeCount = sizeConstraints(cluster, clusterResizeLogBuilder, currentNodes);
      if (newNodeCount == currentNodes) {
        LOGGER.info("Too early to autoscale");
        return;
      } else {
        updateNodeCount(
            session, cluster, timeSupplier, clusterResizeLogBuilder, newNodeCount, currentNodes);
        return;
      }
    }
    final Duration samplingDuration = getSamplingDuration(cluster, timeSupplier);
    int newNodeCount =
        cpuStrategy(cluster, clusterResizeLogBuilder, samplingDuration, currentNodes);
    newNodeCount =
        storageConstraints(
            cluster, clusterResizeLogBuilder, samplingDuration, newNodeCount, currentNodes);
    newNodeCount = frequencyConstraints(cluster, timeSupplier, newNodeCount, currentNodes);
    newNodeCount = sizeConstraints(cluster, clusterResizeLogBuilder, newNodeCount);
    updateNodeCount(
        session, cluster, timeSupplier, clusterResizeLogBuilder, newNodeCount, currentNodes);
  }

  private void updateNodeCount(
      final BigtableSession bigtableSession,
      final BigtableCluster cluster,
      final Supplier<Instant> timeSupplier,
      final ClusterResizeLogBuilder clusterResizeLogBuilder,
      final int desiredNodes,
      final int currentNodes) {
    if (desiredNodes != currentNodes) {
      setSize(bigtableSession, cluster.clusterName(), clusterResizeLogBuilder, desiredNodes);
      database.setLastChange(
          cluster.projectId(), cluster.instanceId(), cluster.clusterId(), timeSupplier.get());
      LOGGER.info("Changing nodes from {} to {}", currentNodes, desiredNodes);
    } else {
      LOGGER.info("No need to resize");
    }
    LOGGER.info("Finished running autoscale job");
    database.clearFailureCount(cluster);
  }
}
