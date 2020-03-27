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

package com.spotify.autoscaler.algorithm;

import com.spotify.autoscaler.ScalingEvent;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.ClusterResizeLogBuilder;
import com.spotify.autoscaler.metric.AutoscalerMetrics;
import com.spotify.autoscaler.metric.ClusterLoadGauges;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CPUAlgorithm implements Algorithm {

  private static final double MAX_REDUCTION_RATIO = 0.7;
  private static final double CPU_OVERLOAD_THRESHOLD = 0.9;
  private static final Logger LOGGER = LoggerFactory.getLogger(CPUAlgorithm.class);
  private final StackdriverClient stackdriverClient;
  private final AutoscalerMetrics autoscalerMetrics;

  public CPUAlgorithm(
      final StackdriverClient stackdriverClient, final AutoscalerMetrics autoscalerMetrics) {
    this.stackdriverClient = stackdriverClient;
    this.autoscalerMetrics = autoscalerMetrics;
  }

  public ScalingEvent calculateWantedNodes(
      final BigtableCluster cluster,
      final ClusterResizeLogBuilder clusterResizeLogBuilder,
      final Duration samplingDuration,
      final int currentNodes) {
    return cpuStrategy(cluster, clusterResizeLogBuilder, samplingDuration, currentNodes);
  }

  private ScalingEvent cpuStrategy(
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
        "Running autoscale job. Nodes: {} (min={}, max={}, minNodesOverride={}), CPU: {} (target={})",
        nodes,
        cluster.minNodes(),
        cluster.maxNodes(),
        cluster.minNodesOverride(),
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
      final double lowestAmountNodes = MAX_REDUCTION_RATIO * nodes;
      final double maxReduction = nodes - lowestAmountNodes;
      desiredNodes =
          maxReduction > 1
              ? Math.max(initialDesiredNodes, Math.ceil(maxReduction))
              : Math.max(initialDesiredNodes, Math.floor(maxReduction));
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
    return new ScalingEvent(roundedDesiredNodes, "cpu-constraint");
  }

  private double getCurrentCpu(final BigtableCluster cluster, final Duration samplingDuration) {
    return stackdriverClient.getCpuLoad(cluster, samplingDuration);
  }
}
