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

import com.spotify.autoscaler.LoggerContext;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.Database;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
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
}
