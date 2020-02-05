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

public class StorageAlgorithm implements Algorithm {

  // Google recommends keeping the disk utilization around 70% to accommodate sudden spikes
  // see <<Storage utilization per node>> section for details,
  // https://cloud.google.com/bigtable/quotas
  private static final double MAX_DISK_UTILIZATION_PERCENTAGE = 0.7d;

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageAlgorithm.class);
  private final StackdriverClient stackdriverClient;
  private final AutoscalerMetrics autoscalerMetrics;

  public StorageAlgorithm(
      final StackdriverClient stackdriverClient, final AutoscalerMetrics autoscalerMetrics) {
    this.stackdriverClient = stackdriverClient;
    this.autoscalerMetrics = autoscalerMetrics;
  }

  public ScalingEvent calculateWantedNodes(
      final BigtableCluster cluster,
      final ClusterResizeLogBuilder clusterResizeLogBuilder,
      final Duration samplingDuration,
      final int currentNodes) {
    return storageConstraints(cluster, clusterResizeLogBuilder, samplingDuration, currentNodes);
  }

  private ScalingEvent storageConstraints(
      final BigtableCluster cluster,
      final ClusterResizeLogBuilder clusterResizeLogBuilder,
      final Duration samplingDuration,
      final int currentNodes) {
    Double storageUtilization = 0.0;
    try {
      storageUtilization = stackdriverClient.getDiskUtilization(cluster, samplingDuration);
    } finally {
      autoscalerMetrics.registerClusterLoadMetrics(
          cluster, storageUtilization, ClusterLoadGauges.STORAGE);
    }
    if (storageUtilization <= 0.0) {
      LOGGER.warn(
          "Storage utilization reported less than or equal to 0.0, not letting any downscale!");
      return new ScalingEvent(currentNodes, "0 storage utilization");
    }
    final int minNodesRequiredForStorage =
        (int) Math.ceil(storageUtilization * currentNodes / MAX_DISK_UTILIZATION_PERCENTAGE);
    LOGGER.info(
        "Minimum nodes for storage: {}, currentUtilization: {}, current nodes: {}",
        minNodesRequiredForStorage,
        storageUtilization.toString(),
        currentNodes);
    clusterResizeLogBuilder.storageUtilization(storageUtilization);

    return new ScalingEvent(minNodesRequiredForStorage, "storage-constraint");
  }
}
