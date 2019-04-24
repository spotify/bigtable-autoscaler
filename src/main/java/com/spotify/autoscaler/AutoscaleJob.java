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
import static com.spotify.autoscaler.Main.APP_PREFIX;
import com.google.bigtable.admin.v2.Cluster;
import com.google.bigtable.admin.v2.GetClusterRequest;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.common.annotations.VisibleForTesting;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.ClusterResizeLogBuilder;
import com.spotify.autoscaler.db.Database;
import com.spotify.metrics.core.SemanticMetricRegistry;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoscaleJob implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(AutoscaleJob.class);

  private final BigtableSession bigtableSession;
  private final StackdriverClient stackdriverClient;
  private final BigtableCluster cluster;
  private final SemanticMetricRegistry registry;
  private final Supplier<Instant> timeSource;
  private final Database db;
  private final ClusterStats clusterStats;

  public static final Duration CHECK_INTERVAL = Duration.ofSeconds(30);
  private boolean hasRun = false;
  private ClusterResizeLogBuilder log;
  private StringBuilder resizeReason = new StringBuilder();

  // CPU related constants
  private static final double MAX_REDUCTION_RATIO = 0.7;
  private static final double CPU_OVERLOAD_THRESHOLD = 0.9;
  private static final Duration MAX_SAMPLE_INTERVAL = Duration.ofHours(1);

  // Google recommends keeping the disk utilization around 70% to accomodate sudden spikes
  // see <<Storage utilization per node>> section for details, https://cloud.google.com/bigtable/quotas
  private static final double MAX_DISK_UTILIZATION_PERCENTAGE = 0.7d;

  // Time related constants
  private static final Duration AFTER_CHANGE_SAMPLE_BUFFER_TIME = Duration.ofMinutes(5);
  public static final Duration RESIZE_SETTLE_TIME = Duration.ofMinutes(5);
  public static final Duration MINIMUM_CHANGE_INTERVAL = RESIZE_SETTLE_TIME.plus(AFTER_CHANGE_SAMPLE_BUFFER_TIME);
  public static final Duration MAX_FAILURE_BACKOFF_INTERVAL = Duration.ofHours(4);
  // This means that a 2 % capacity increase can only happen every second hour,
  // but a twenty percent change can happen as often as every twelve minutes.
  public static final double MINIMUM_UPSCALE_WEIGHT = 14400;
  // This means that a 2 % capacity decrease can only happen every eight hours,
  // but a twenty percent change can happen as often as every 48 minutes.
  public static final double MINIMUM_DOWNSACLE_WEIGHT = 57600;

  // Storage related constants

  public AutoscaleJob(final BigtableSession bigtableSession,
                      final StackdriverClient stackdriverClient,
                      final BigtableCluster cluster,
                      final Database db,
                      final SemanticMetricRegistry registry,
                      final ClusterStats clusterStats,
                      final Supplier<Instant> timeSource) {
    this.bigtableSession = checkNotNull(bigtableSession);
    this.stackdriverClient = checkNotNull(stackdriverClient);
    this.cluster = checkNotNull(cluster);
    this.registry = checkNotNull(registry);
    this.clusterStats = checkNotNull(clusterStats);
    this.timeSource = checkNotNull(timeSource);
    this.db = checkNotNull(db);
    log = new ClusterResizeLogBuilder()
        .timestamp(new Date())
        .projectId(cluster.projectId())
        .instanceId(cluster.instanceId())
        .clusterId(cluster.clusterId())
        .minNodes(cluster.minNodes())
        .maxNodes(cluster.maxNodes())
        .cpuTarget(cluster.cpuTarget())
        .overloadStep(cluster.overloadStep())
        .loadDelta(cluster.loadDelta());
  }

  int getSize(Cluster clusterInfo) {
    registry.meter(APP_PREFIX.tagged("what", "call-to-get-size")).mark();
    int currentNodes = clusterInfo.getServeNodes();
    log.currentNodes(currentNodes);
    return currentNodes;
  }

  Cluster getClusterInfo() throws IOException{
    BigtableInstanceClient adminClient = bigtableSession.getInstanceAdminClient();
    return adminClient.getCluster(
          GetClusterRequest.newBuilder()
              .setName(this.cluster.clusterName())
              .build());
  }

  void setSize(int newSize) {
    final Cluster cluster = Cluster.newBuilder()
        .setName(this.cluster.clusterName())
        .setServeNodes(newSize)
        .build();

    // two separate metrics here;
    // clusters-changed would only reflect the ones that are successfully changed, while
    // call-to-set-size would reflect the number of API calls, to monitor if we are reaching the daily limit
    registry.meter(APP_PREFIX.tagged("what", "call-to-set-size")).mark();
    try {
      log.targetNodes(newSize);
      bigtableSession.getInstanceAdminClient().updateCluster(cluster);
      log.success(true);
      registry.meter(APP_PREFIX.tagged("what", "clusters-changed")).mark();
    } catch (IOException e) {
      logger.error("Failed to set cluster size", e);
      log.errorMessage(Optional.of(e.toString()));
      log.success(false);
      registry.meter(APP_PREFIX.tagged("what", "set-size-transport-error")).mark();
    } catch (Throwable t) {
      log.errorMessage(Optional.of(t.toString()));
      log.success(false);
      throw t;
    } finally {
      log.resizeReason(resizeReason.toString());
      db.logResize(log.build());
    }
  }

  Duration getDurationSinceLastChange() {
    // the database always stores UTC time
    // so remember to use UTC time as well when comparing with the database
    // for example Instant.now() returns UTC time
    Instant now = timeSource.get();
    Instant lastChange = cluster.lastChange().orElse(Instant.EPOCH);
    return Duration.between(lastChange, now);
  }

  Duration getSamplingDuration() {
    Duration timeSinceLastChange = getDurationSinceLastChange();
    return computeSamplingDuration(timeSinceLastChange);
  }

  Duration computeSamplingDuration(final Duration timeSinceLastChange) {
    final Duration reducedTimeSinceLastChange = timeSinceLastChange.minus(AFTER_CHANGE_SAMPLE_BUFFER_TIME);
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
  @VisibleForTesting
  double getCurrentCpu(final Duration samplingDuration) {
    return stackdriverClient.getCpuLoad(cluster, samplingDuration);
  }

  int cpuStrategy(final Duration samplingDuration, int nodes) {
    double currentCpu = 0d;

    try {
      currentCpu = getCurrentCpu(samplingDuration);
    } finally {
      clusterStats.setLoad(cluster, currentCpu, ClusterStats.MetricType.CPU);
    }

    logger.info(
        "Running autoscale job. Nodes: {} (min={}, max={}, loadDelta={}), CPU: {} (target={})",
        nodes, cluster.minNodes(), cluster.maxNodes(), cluster.loadDelta(),
        currentCpu, cluster.cpuTarget());

    double initialDesiredNodes = currentCpu * nodes / cluster.cpuTarget();

    final double desiredNodes;
    boolean scaleDown = (initialDesiredNodes < nodes);
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

    int roundedDesiredNodes = (int) Math.ceil(desiredNodes);
    logger.info("Ideal node count: {}. Revised nodes: {}. Reason: {}.", initialDesiredNodes, desiredNodes, path);
    log.cpuUtilization(currentCpu);
    addResizeReason("CPU strategy: " + path);
    return roundedDesiredNodes;
  }

  boolean shouldExponentialBackoff() {
    Instant now = timeSource.get();

    if (cluster.lastFailure().isPresent() && cluster.consecutiveFailureCount() > 0) {
      // Last try resulted in a failure. Exponential backoff further tries.
      // After the first failure, the next attempt will be 1 minute after the failure, then 2, 4, 8 etc minutes
      // But never more than 4 hours.
      Duration nextTryDuration = MAX_FAILURE_BACKOFF_INTERVAL;
      if (cluster.consecutiveFailureCount() < 10) {
        // 9 consecutive failures would result in a duration ~4.3 hours,
        // so no need to calculate the exponential duration for anything >9
        nextTryDuration = CHECK_INTERVAL.multipliedBy((long) Math.pow(2, cluster.consecutiveFailureCount()));
        if (nextTryDuration.compareTo(MAX_FAILURE_BACKOFF_INTERVAL) > 0) {
          nextTryDuration = MAX_FAILURE_BACKOFF_INTERVAL;
        }
      }

      Instant nextTry = cluster.lastFailure().get().plus(nextTryDuration);
      if (nextTry.isAfter(now)) {
        logger.info("Skipping autoscale check due to earlier failures; exponential backoff - next try at {}", nextTry);
        return true;
      }
    }

    return false;
  }

  int storageConstraints(final Duration samplingDuration, int desiredNodes, int currentNodes) {

    Double storageUtilization = 0.0;
    try {
      storageUtilization = stackdriverClient.getDiskUtilization(cluster, samplingDuration);
    } finally {
      clusterStats.setLoad(cluster, storageUtilization, ClusterStats.MetricType.STORAGE);
    }
    if (storageUtilization <= 0.0) {
      return Math.max(currentNodes, desiredNodes);
    }
    int minNodesRequiredForStorage =
        (int) Math.ceil(storageUtilization * currentNodes / MAX_DISK_UTILIZATION_PERCENTAGE);
    logger.info("Minimum nodes for storage: {}, currentUtilization: {}, current nodes: {}",
        minNodesRequiredForStorage, storageUtilization.toString(), currentNodes);
    log.storageUtilization(storageUtilization);
    if (minNodesRequiredForStorage > desiredNodes) {
      addResizeReason(String.format("Storage strategy: Target node count overriden(%d -> %d).", desiredNodes,
          minNodesRequiredForStorage));
    }
    return Math.max(minNodesRequiredForStorage, desiredNodes);
  }

  int sizeConstraints(int desiredNodes) {

    // the desired size should be inside the autoscale boundaries
    int finalNodes = Math.max(cluster.effectiveMinNodes(), Math.min(cluster.maxNodes(), desiredNodes));
    if (desiredNodes != finalNodes) {
      addResizeReason(String.format("Size strategy: Target count overriden(%d -> %d)", desiredNodes, finalNodes));
    }
    return finalNodes;
  }

  boolean isTooEarlyToFetchMetrics() {
    Duration timeSinceLastChange = getDurationSinceLastChange();
    return timeSinceLastChange.minus(MINIMUM_CHANGE_INTERVAL).isNegative();
  }

  // Implements a stretegy to avoid autoscaling too often
  int frequencyConstraints(int nodes, int currentNodes) {
    Duration timeSinceLastChange = getDurationSinceLastChange();
    int desiredNodes = nodes;
    // It's OK to do large changes often if needed, but only do small changes very rarely to avoid too much oscillation
    double changeWeight =
        100.0 * Math.abs(1.0 - (double) desiredNodes / currentNodes) * timeSinceLastChange.getSeconds();
    boolean scaleDown = (desiredNodes < currentNodes);
    String path = "normal";

    if (scaleDown && (changeWeight < MINIMUM_DOWNSACLE_WEIGHT)) {
      // Avoid downscaling too frequently
      path = "downscale too small/frequent";
      desiredNodes = currentNodes;
    } else if (!scaleDown && (changeWeight < MINIMUM_UPSCALE_WEIGHT)) {
      // Avoid upscaling too frequently
      path = "upscale too small/frequent";
      desiredNodes = currentNodes;
    }

    logger.info("Ideal node count: {}. Revised nodes: {}. Reason: {}.", nodes, desiredNodes, path);
    return desiredNodes;
  }

  void run() throws IOException {

    if (hasRun) {
      throw new RuntimeException("An autoscale job should only be run once!");
    }

    hasRun = true;

    if (shouldExponentialBackoff()) {
      logger.info("Exponential backoff");
      return;
    }

    final Cluster clusterInfo = getClusterInfo();
    int currentNodes = getSize(clusterInfo);
    clusterStats.setStats(this.cluster, currentNodes);

    registry.meter(APP_PREFIX.tagged("what", "clusters-checked")).mark();

    if (isTooEarlyToFetchMetrics()) {
      int newNodeCount = sizeConstraints(currentNodes);
      if (newNodeCount == currentNodes) {
        logger.info("Too early to autoscale");
        return;
      } else {
        updateNodeCount(newNodeCount, currentNodes);
        return;
      }
    }
    final Duration samplingDuration = getSamplingDuration();
    int newNodeCount = cpuStrategy(samplingDuration, currentNodes);
    newNodeCount = storageConstraints(samplingDuration, newNodeCount, currentNodes);
    newNodeCount = frequencyConstraints(newNodeCount, currentNodes);
    newNodeCount = sizeConstraints(newNodeCount);
    updateNodeCount(newNodeCount, currentNodes);
  }

  void updateNodeCount(int desiredNodes, int currentNodes) {
    if (desiredNodes != currentNodes) {
      setSize(desiredNodes);
      db.setLastChange(cluster.projectId(), cluster.instanceId(), cluster.clusterId(), timeSource.get());
      logger.info("Changing nodes from {} to {}", currentNodes, desiredNodes);
    } else {
      logger.info("No need to resize");
    }
    logger.info("Finished running autoscale job");
    db.clearFailureCount(cluster.projectId(), cluster.instanceId(), cluster.clusterId());
  }

  private void addResizeReason(String reason) {
    resizeReason.insert(0, reason);
    resizeReason.insert(0, " >>");
  }

  public void close() throws IOException {
    bigtableSession.close();
  }
}
