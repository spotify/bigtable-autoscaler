package com.spotify.autoscaler;

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

  public CPUAlgorithm(final StackdriverClient stackdriverClient, final AutoscalerMetrics autoscalerMetrics) {
    this.stackdriverClient = stackdriverClient;
    this.autoscalerMetrics = autoscalerMetrics;
  }


  public ScalingEvent calculateWantedNodes(final BigtableCluster cluster,
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
    return new ScalingEvent(roundedDesiredNodes, "CPU strategy required nodes");
  }


  private double getCurrentCpu(final BigtableCluster cluster, final Duration samplingDuration) {
    return stackdriverClient.getCpuLoad(cluster, samplingDuration);
  }

}


