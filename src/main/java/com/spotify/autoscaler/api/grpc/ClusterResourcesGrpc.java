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
import com.spotify.autoscaler.LoggerContext;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.Database;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterResourcesGrpc
    extends ClusterAutoscalerConfigurationGrpc.ClusterAutoscalerConfigurationImplBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterResourcesGrpc.class);
  private final Database database;

  @Inject
  public ClusterResourcesGrpc(final Database database) {
    this.database = Objects.requireNonNull(database);
  }

  @Override
  public void enabled(
      final ClusterIdentifier request,
      final StreamObserver<ClusterEnabledResponse> responseObserver) {

    final Optional<BigtableCluster> cluster =
        database.getBigtableCluster(
            request.getProjectId(), request.getInstanceId(), request.getClusterId());
    if (cluster.isPresent()) {
      responseObserver.onNext(
          ClusterEnabledResponse.newBuilder().setIsEnabled(cluster.get().enabled()).build());
      responseObserver.onCompleted();
    } else {
      responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND));
    }
  }

  @Override
  public StreamObserver<ClusterMinNodesOverrideRequest> setMinNodesOverride(
      final StreamObserver<ClusterMinNodesOverrideResponse> responseObserver) {

    return new StreamObserver<ClusterMinNodesOverrideRequest>() {
      @Override
      public void onNext(final ClusterMinNodesOverrideRequest clusterMinNodesOverrideRequest) {

        final String projectId = clusterMinNodesOverrideRequest.getCluster().getProjectId();
        final String instanceId = clusterMinNodesOverrideRequest.getCluster().getInstanceId();
        final String clusterId = clusterMinNodesOverrideRequest.getCluster().getClusterId();
        final int minNodesOverride = clusterMinNodesOverrideRequest.getMinNodesOverride();

        final BigtableCluster cluster =
            new BigtableClusterBuilder()
                .projectId(projectId)
                .instanceId(instanceId)
                .clusterId(clusterId)
                .minNodesOverride(minNodesOverride)
                .build();
        try {
          LoggerContext.pushContext(cluster);
          final Optional<BigtableCluster> maybeCluster =
              database.getBigtableCluster(projectId, instanceId, clusterId);

          if (maybeCluster.isPresent()) {
            if (database.setMinNodesOverride(projectId, instanceId, clusterId, minNodesOverride)) {
              LOGGER.info("cluster minNodesOverride updated to {}", minNodesOverride);
              responseObserver.onNext(
                  ClusterMinNodesOverrideResponse.newBuilder()
                      .setOriginalRequest(clusterMinNodesOverrideRequest)
                      .setStatus(
                          com.google.rpc.Status.newBuilder()
                              .setCode(Status.Code.OK.value())
                              .build())
                      .build());
            } else {
              responseObserver.onNext(
                  ClusterMinNodesOverrideResponse.newBuilder()
                      .setOriginalRequest(clusterMinNodesOverrideRequest)
                      .setStatus(
                          com.google.rpc.Status.newBuilder()
                              .setCode(Status.Code.INVALID_ARGUMENT.value())
                              .build())
                      .build());
            }
          } else {
            responseObserver.onNext(
                ClusterMinNodesOverrideResponse.newBuilder()
                    .setOriginalRequest(clusterMinNodesOverrideRequest)
                    .setStatus(
                        com.google.rpc.Status.newBuilder()
                            .setCode(Status.Code.NOT_FOUND.value())
                            .build())
                    .build());
          }
        } finally {
          LoggerContext.clearContext();
        }
      }

      @Override
      public void onError(final Throwable throwable) {
        LOGGER.error("ClusterResourcesGrpc error in request stream", throwable);
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
      }
    };
  }

  @Override
  public void create(
      final AutoscalerConfiguration request,
      final StreamObserver<com.google.rpc.Status> responseObserver) {
    super.create(request, responseObserver);
  }

  @Override
  public void update(
      final AutoscalerConfiguration request,
      final StreamObserver<com.google.rpc.Status> responseObserver) {
    super.update(request, responseObserver);
  }

  @Override
  public void delete(
      final ClusterIdentifier request,
      final StreamObserver<com.google.rpc.Status> responseObserver) {
    super.delete(request, responseObserver);
  }

  @Override
  public void get(
      final OptionalClusterIdentifier request,
      final StreamObserver<ClusterAutoscalerInfoList> responseObserver) {

    final String projectId = request.hasProjectId() ? request.getProjectId().getValue() : null;
    final String instanceId = request.hasInstanceId() ? request.getInstanceId().getValue() : null;
    final String clusterId = request.hasClusterId() ? request.getClusterId().getValue() : null;

    final List<BigtableCluster> bigtableClusters =
        database.getBigtableClusters(projectId, instanceId, clusterId);

    final ClusterAutoscalerInfoList.Builder builder = ClusterAutoscalerInfoList.newBuilder();

    bigtableClusters
        .stream()
        .map(ClusterResourcesGrpc::convertToClusterAutoscalerInfo)
        .forEach(builder::addClusterAutoscalerInfo);

    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void getLogs(
      final ClusterIdentifier request,
      final StreamObserver<ClusterAutoscalerLogs> responseObserver) {
    super.getLogs(request, responseObserver);
  }

  @VisibleForTesting
  public static ClusterAutoscalerInfo convertToClusterAutoscalerInfo(
      final BigtableCluster cluster) {

    final ClusterIdentifier clusterIdentifier =
        ClusterIdentifier.newBuilder()
            .setProjectId(cluster.projectId())
            .setInstanceId(cluster.instanceId())
            .setClusterId(cluster.clusterId())
            .build();
    final AutoscalerConfiguration.Builder configBuilder =
        AutoscalerConfiguration.newBuilder()
            .setCluster(clusterIdentifier)
            .setMinNodes(cluster.minNodes())
            .setMaxNodes(cluster.maxNodes())
            .setCpuTarget(cluster.cpuTarget())
            .setEnabled(BoolValue.of(cluster.enabled()))
            .setMinNodesOverride(Int32Value.of(cluster.minNodesOverride()));

    cluster
        .overloadStep()
        .ifPresent(overloadStep -> configBuilder.setOverloadStep(Int32Value.of(overloadStep)));

    final AutoscalerConfiguration config = configBuilder.build();

    final ClusterAutoscalerInfo.Builder autoscalerInfoBuilder =
        ClusterAutoscalerInfo.newBuilder()
            .setConfiguration(config)
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
}
