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

package com.spotify.autoscaler.api.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.util.Timestamps;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.ClusterResizeLog;
import java.util.Optional;

public class MessageConverters {

  @VisibleForTesting
  public static ClusterAutoscalerLog convertToClusterClusterAutoscalerLog(
      final ClusterResizeLog clusterResizeLog) {

    final ClusterAutoscalerLog.Builder builder =
        ClusterAutoscalerLog.newBuilder()
            .setConfiguration(convertToAutoscalerConfiguration(clusterResizeLog))
            .setCpuUtilization(clusterResizeLog.cpuUtilization())
            .setCurrentNodes(clusterResizeLog.currentNodes())
            .setSuccess(clusterResizeLog.success())
            .setTargetNodes(clusterResizeLog.targetNodes())
            .setTimestamp(Timestamps.fromMillis(clusterResizeLog.timestamp().getTime()))
            .setStorageUtilization(clusterResizeLog.storageUtilization());

    if (clusterResizeLog.resizeReason() != null) {
      builder.setResizeReason(StringValue.of(clusterResizeLog.resizeReason()));
    }
    clusterResizeLog
        .errorMessage()
        .ifPresent(errorMessage -> builder.setErrorMessage(StringValue.of(errorMessage)));
    return builder.build();
  }

  private static BigtableClusterBuilder convertToBigtableClusterBuilder(
      final ClusterIdentifier clusterIdentifier) {
    return new BigtableClusterBuilder()
        .projectId(clusterIdentifier.getProjectId())
        .instanceId(clusterIdentifier.getInstanceId())
        .clusterId(clusterIdentifier.getClusterId());
  }

  static BigtableCluster convertToBigtableCluster(final ClusterIdentifier clusterIdentifier) {
    return convertToBigtableClusterBuilder(clusterIdentifier)
        .minNodes(0)
        .maxNodes(0)
        .cpuTarget(0)
        .overloadStep(Optional.of(0))
        .enabled(true)
        .minNodesOverride(0)
        .build();
  }

  @VisibleForTesting
  public static ClusterAutoscalerInfo convertToClusterAutoscalerInfo(
      final BigtableCluster cluster) {

    final ClusterAutoscalerInfo.Builder autoscalerInfoBuilder =
        ClusterAutoscalerInfo.newBuilder()
            .setConfiguration(convertToAutoscalerConfiguration(cluster))
            .setConsecutiveFailureCount(cluster.consecutiveFailureCount());

    cluster
        .lastChange()
        .ifPresent(
            lastChange ->
                autoscalerInfoBuilder.setLastChange(
                    Timestamps.fromMillis(lastChange.toEpochMilli())));
    cluster
        .lastCheck()
        .ifPresent(
            lastCheck ->
                autoscalerInfoBuilder.setLastCheck(
                    Timestamps.fromMillis(lastCheck.toEpochMilli())));
    cluster
        .lastFailure()
        .ifPresent(
            lastFailure ->
                autoscalerInfoBuilder.setLastFailure(
                    Timestamps.fromMillis(lastFailure.toEpochMilli())));
    cluster
        .lastFailureMessage()
        .ifPresent(
            lastFailureMessage ->
                autoscalerInfoBuilder.setLastFailureMessage(StringValue.of(lastFailureMessage)));
    cluster
        .errorCode()
        .ifPresent(
            errorCode -> autoscalerInfoBuilder.setErrorCode(StringValue.of(errorCode.name())));

    return autoscalerInfoBuilder.build();
  }

  private static ClusterIdentifier convertToClusterIdentifier(final BigtableCluster cluster) {
    return ClusterIdentifier.newBuilder()
        .setProjectId(cluster.projectId())
        .setInstanceId(cluster.instanceId())
        .setClusterId(cluster.clusterId())
        .build();
  }

  private static ClusterIdentifier convertToClusterIdentifier(
      final ClusterResizeLog clusterResizeLog) {
    return ClusterIdentifier.newBuilder()
        .setProjectId(clusterResizeLog.projectId())
        .setInstanceId(clusterResizeLog.instanceId())
        .setClusterId(clusterResizeLog.clusterId())
        .build();
  }

  private static AutoscalerConfiguration convertToAutoscalerConfiguration(
      final BigtableCluster cluster) {
    final AutoscalerConfiguration.Builder configBuilder =
        AutoscalerConfiguration.newBuilder()
            .setCluster(convertToClusterIdentifier(cluster))
            .setMinNodes(cluster.minNodes())
            .setMaxNodes(cluster.maxNodes())
            .setCpuTarget(cluster.cpuTarget())
            .setEnabled(BoolValue.of(cluster.enabled()))
            .setMinNodesOverride(Int32Value.of(cluster.minNodesOverride()));

    cluster
        .overloadStep()
        .ifPresent(overloadStep -> configBuilder.setOverloadStep(Int32Value.of(overloadStep)));

    return configBuilder.build();
  }

  private static AutoscalerConfiguration convertToAutoscalerConfiguration(
      final ClusterResizeLog clusterResizeLog) {
    final AutoscalerConfiguration.Builder configBuilder =
        AutoscalerConfiguration.newBuilder()
            .setCluster(convertToClusterIdentifier(clusterResizeLog))
            .setMinNodes(clusterResizeLog.minNodes())
            .setMaxNodes(clusterResizeLog.maxNodes())
            .setCpuTarget(clusterResizeLog.cpuTarget())
            .setMinNodesOverride(Int32Value.of(clusterResizeLog.minNodesOverride()));

    clusterResizeLog
        .overloadStep()
        .ifPresent(overloadStep -> configBuilder.setOverloadStep(Int32Value.of(overloadStep)));

    return configBuilder.build();
  }
}
