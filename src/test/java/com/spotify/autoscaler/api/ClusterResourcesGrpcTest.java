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

package com.spotify.autoscaler.api;

import static com.spotify.autoscaler.api.ApiTestResources.CLUSTERS;
import static com.spotify.autoscaler.api.ApiTestResources.DISABLED_CLUSTER;
import static com.spotify.autoscaler.api.ApiTestResources.ENABLED_CLUSTER;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.spotify.autoscaler.api.grpc.ClusterAutoscalerConfigurationGrpc;
import com.spotify.autoscaler.api.grpc.ClusterEnabledResponse;
import com.spotify.autoscaler.api.grpc.ClusterIdentifier;
import com.spotify.autoscaler.api.grpc.ClusterMinNodesOverrideRequest;
import com.spotify.autoscaler.api.grpc.ClusterMinNodesOverrideResponse;
import com.spotify.autoscaler.api.grpc.ClusterResourcesGrpc;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.Database;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

public class ClusterResourcesGrpcTest {

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Rule public final ExpectedException exception = ExpectedException.none();

  @Mock private Database db;

  private ClusterAutoscalerConfigurationGrpc.ClusterAutoscalerConfigurationStub stub;
  private ClusterAutoscalerConfigurationGrpc.ClusterAutoscalerConfigurationBlockingStub
      blockingStub;

  private static final ClusterIdentifier ENABLED_CLUSTER_IDENTIFIER =
      createClusterIdentifier(ENABLED_CLUSTER);

  private static final ClusterIdentifier DISABLED_CLUSTER_IDENTIFIER =
      createClusterIdentifier(DISABLED_CLUSTER);

  private static final ClusterIdentifier NONEXISTING_CLUSTER_IDENTIFIER =
      ClusterIdentifier.newBuilder()
          .setProjectId("nonExistingProject")
          .setInstanceId("nonExistingInstance")
          .setClusterId("nonExistingCluster")
          .build();

  @Before
  public void configure() throws IOException {
    initMocks(this);
    when(db.getBigtableCluster(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              final String projectId = invocation.getArgument(0);
              final String instanceId = invocation.getArgument(1);
              final String clusterId = invocation.getArgument(2);

              for (BigtableCluster cluster : CLUSTERS) {
                if (projectId.equals(cluster.projectId())
                    && instanceId.equals(cluster.instanceId())
                    && clusterId.equals(cluster.clusterId())) {
                  return Optional.of(cluster);
                }
              }
              return Optional.empty();
            });

    when(db.setMinNodesOverride(any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              final String projectId = invocation.getArgument(0);
              final String instanceId = invocation.getArgument(1);
              final String clusterId = invocation.getArgument(2);
              final int minNodesOverride = invocation.getArgument(3);

              for (BigtableCluster cluster : CLUSTERS) {
                if (projectId.equals(cluster.projectId())
                    && instanceId.equals(cluster.instanceId())
                    && clusterId.equals(cluster.clusterId())) {
                  return minNodesOverride >= 0;
                }
              }
              return false;
            });

    String serverName = InProcessServerBuilder.generateName();

    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new ClusterResourcesGrpc(db))
            .build()
            .start());

    blockingStub =
        ClusterAutoscalerConfigurationGrpc.newBlockingStub(
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    stub =
        ClusterAutoscalerConfigurationGrpc.newStub(
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));
  }

  @Test
  public void testEnabledCluster() {
    final ClusterEnabledResponse enabled = blockingStub.enabled(ENABLED_CLUSTER_IDENTIFIER);
    assertTrue(enabled.getIsEnabled());
  }

  @Test
  public void testDisabledCluster() {

    final ClusterEnabledResponse enabled = blockingStub.enabled(DISABLED_CLUSTER_IDENTIFIER);
    assertFalse(enabled.getIsEnabled());
  }

  @Test
  public void testNonExistingCluster() {
    exception.expect(StatusRuntimeException.class);
    exception.expectMessage(Status.NOT_FOUND.getCode().toString());
    blockingStub.enabled(NONEXISTING_CLUSTER_IDENTIFIER);
  }

  private static ClusterIdentifier createClusterIdentifier(final BigtableCluster cluster) {
    return ClusterIdentifier.newBuilder()
        .setProjectId(cluster.projectId())
        .setInstanceId(cluster.instanceId())
        .setClusterId(cluster.clusterId())
        .build();
  }

  private static ClusterMinNodesOverrideRequest createClusterMinNodesOverrideRequest(
      final ClusterIdentifier cluster, int minNodesOverride) {
    return ClusterMinNodesOverrideRequest.newBuilder()
        .setCluster(cluster)
        .setMinNodesOverride(minNodesOverride)
        .build();
  }

  @Test
  public void testClusterMinNodesOverrideStream() throws InterruptedException {

    final Map<ClusterMinNodesOverrideRequest, Status> expectedStatuses = new HashMap<>();

    for (BigtableCluster cluster : CLUSTERS) {
      expectedStatuses.put(
          createClusterMinNodesOverrideRequest(createClusterIdentifier(cluster), -1),
          Status.INVALID_ARGUMENT);
      expectedStatuses.put(
          createClusterMinNodesOverrideRequest(createClusterIdentifier(cluster), 0), Status.OK);
      expectedStatuses.put(
          createClusterMinNodesOverrideRequest(createClusterIdentifier(cluster), 5), Status.OK);
    }

    expectedStatuses.put(
        createClusterMinNodesOverrideRequest(NONEXISTING_CLUSTER_IDENTIFIER, -1), Status.NOT_FOUND);
    expectedStatuses.put(
        createClusterMinNodesOverrideRequest(NONEXISTING_CLUSTER_IDENTIFIER, 0), Status.NOT_FOUND);
    expectedStatuses.put(
        createClusterMinNodesOverrideRequest(NONEXISTING_CLUSTER_IDENTIFIER, 5), Status.NOT_FOUND);

    final CountDownLatch latch = new CountDownLatch(1);
    final Map<ClusterMinNodesOverrideRequest, com.google.rpc.Status> realStatuses = new HashMap<>();

    final StreamObserver<ClusterMinNodesOverrideResponse> responseObserver =
        new StreamObserver<ClusterMinNodesOverrideResponse>() {
          @Override
          public void onNext(final ClusterMinNodesOverrideResponse response) {
            realStatuses.put(response.getOriginalRequest(), response.getStatus());
          }

          @Override
          public void onError(final Throwable throwable) {
            fail();
          }

          @Override
          public void onCompleted() {
            latch.countDown();
          }
        };

    final StreamObserver<ClusterMinNodesOverrideRequest> requestObserver =
        stub.setMinNodesOverride(responseObserver);

    expectedStatuses.forEach((key, value) -> requestObserver.onNext(key));

    requestObserver.onCompleted();

    assertTrue(latch.await(1, TimeUnit.SECONDS));

    expectedStatuses.forEach(
        (key, value) -> {
          final int code = realStatuses.get(key).getCode();
          assertEquals(key.toString(), value.getCode().value(), code);
        });
  }
}
