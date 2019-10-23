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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.protobuf.StringValue;
import com.google.protobuf.util.Timestamps;
import com.spotify.autoscaler.api.grpc.AutoscalerConfiguration;
import com.spotify.autoscaler.api.grpc.ClusterAutoscalerConfigurationGrpc;
import com.spotify.autoscaler.api.grpc.ClusterAutoscalerInfo;
import com.spotify.autoscaler.api.grpc.ClusterAutoscalerInfoList;
import com.spotify.autoscaler.api.grpc.ClusterAutoscalerLog;
import com.spotify.autoscaler.api.grpc.ClusterEnabledResponse;
import com.spotify.autoscaler.api.grpc.ClusterIdentifier;
import com.spotify.autoscaler.api.grpc.ClusterMinNodesOverrideRequest;
import com.spotify.autoscaler.api.grpc.ClusterMinNodesOverrideResponse;
import com.spotify.autoscaler.api.grpc.ClusterResourcesGrpc;
import com.spotify.autoscaler.api.grpc.MessageConverters;
import com.spotify.autoscaler.api.grpc.OptionalClusterIdentifier;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.ClusterResizeLog;
import com.spotify.autoscaler.db.Database;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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

  private static final AutoscalerConfiguration INVALID_CONFIGURATION =
      AutoscalerConfiguration.newBuilder()
          .setCluster(ClusterIdentifier.newBuilder().setProjectId("invalid").build())
          .build();

  private static final Throwable SQL_EXCEPTION = new SQLException("Invalid data!!");

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

    when(db.getBigtableClusters(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              final String projectId = invocation.getArgument(0);
              final String instanceId = invocation.getArgument(1);
              final String clusterId = invocation.getArgument(2);
              List<BigtableCluster> clusters = new ArrayList<>();
              for (BigtableCluster cluster : CLUSTERS) {
                if ((projectId == null || projectId.equals(cluster.projectId()))
                    && (instanceId == null || instanceId.equals(cluster.instanceId()))
                    && (clusterId == null || clusterId.equals(cluster.clusterId()))) {
                  clusters.add(cluster);
                }
              }
              return clusters;
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

    when(db.deleteBigtableCluster(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              final String projectId = invocation.getArgument(0);
              final String instanceId = invocation.getArgument(1);
              final String clusterId = invocation.getArgument(2);
              for (BigtableCluster cluster : CLUSTERS) {
                if (projectId.equals(cluster.projectId())
                    && instanceId.equals(cluster.instanceId())
                    && clusterId.equals(cluster.clusterId())) {
                  return true;
                }
              }
              return false;
            });

    when(db.insertBigtableCluster(any()))
        .thenAnswer(
            invocation -> {
              final BigtableCluster bigtableCluster = invocation.getArgument(0);
              if (bigtableCluster
                  .projectId()
                  .equals(INVALID_CONFIGURATION.getCluster().getProjectId())) {
                throw SQL_EXCEPTION;
              }
              return true;
            });

    when(db.updateBigtableCluster(any()))
        .thenAnswer(
            invocation -> {
              final BigtableCluster bigtableCluster = invocation.getArgument(0);
              if (bigtableCluster
                  .projectId()
                  .equals(INVALID_CONFIGURATION.getCluster().getProjectId())) {
                throw SQL_EXCEPTION;
              }
              return true;
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
  public void testConvertToClusterAutoscalerInfoFromClusterResizeLog() {

    final ClusterResizeLog clusterResizeLog =
        ClusterResizeLog.builder(ENABLED_CLUSTER)
            .resizeReason("i have my reasons")
            .errorMessage("fail")
            .success(false)
            .cpuUtilization(0.9)
            .storageUtilization(0.7)
            .currentNodes(10)
            .targetNodes(15)
            .timestamp(new Date())
            .build();
    final ClusterAutoscalerLog clusterAutoscalerLog =
        MessageConverters.convertToClusterClusterAutoscalerLog(clusterResizeLog);
    testEquivalent(clusterAutoscalerLog, clusterResizeLog);
  }

  private void testEquivalent(
      final ClusterAutoscalerLog clusterAutoscalerLog, final ClusterResizeLog clusterResizeLog) {

    final AutoscalerConfiguration configuration = clusterAutoscalerLog.getConfiguration();
    assertEquals(clusterResizeLog.projectId(), configuration.getCluster().getProjectId());
    assertEquals(clusterResizeLog.instanceId(), configuration.getCluster().getInstanceId());
    assertEquals(clusterResizeLog.clusterId(), configuration.getCluster().getClusterId());
    assertEquals(clusterResizeLog.cpuTarget(), configuration.getCpuTarget(), 0.001);
    assertFalse(configuration.hasEnabled());
    assertEquals(clusterResizeLog.minNodes(), configuration.getMinNodes());
    assertEquals(clusterResizeLog.maxNodes(), configuration.getMaxNodes());
    assertEquals(
        clusterResizeLog.minNodesOverride(), configuration.getMinNodesOverride().getValue());

    Optional<Integer> overloadStep =
        Optional.ofNullable(
            configuration.hasOverloadStep() ? configuration.getOverloadStep().getValue() : null);
    assertEquals(clusterResizeLog.overloadStep(), overloadStep);

    if (clusterResizeLog.errorMessage().isPresent()) {
      assertEquals(
          clusterResizeLog.errorMessage().get(), clusterAutoscalerLog.getErrorMessage().getValue());
    } else {
      assertFalse(clusterAutoscalerLog.hasErrorMessage());
    }
    assertEquals(
        clusterResizeLog.timestamp().getTime(),
        Timestamps.toMillis(clusterAutoscalerLog.getTimestamp()));

    assertEquals(
        clusterResizeLog.resizeReason(), clusterAutoscalerLog.getResizeReason().getValue());
    assertEquals(clusterResizeLog.currentNodes(), clusterAutoscalerLog.getCurrentNodes());
    assertEquals(clusterResizeLog.targetNodes(), clusterAutoscalerLog.getTargetNodes());
    assertEquals(
        clusterResizeLog.cpuUtilization(), clusterAutoscalerLog.getCpuUtilization(), 0.001);
    assertEquals(
        clusterResizeLog.storageUtilization(), clusterAutoscalerLog.getStorageUtilization(), 0.001);
    assertEquals(clusterResizeLog.success(), clusterAutoscalerLog.getSuccess());
  }

  @Test
  public void testConvertToClusterAutoscalerInfoFromBigtableCluster() {
    final ClusterAutoscalerInfo clusterAutoscalerInfo =
        MessageConverters.convertToClusterAutoscalerInfo(ENABLED_CLUSTER);
    testEquivalent(clusterAutoscalerInfo, ENABLED_CLUSTER);
  }

  private void testEquivalent(
      final ClusterAutoscalerInfo clusterAutoscalerInfo, final BigtableCluster bigtableCluster) {

    if (bigtableCluster.lastChange().isPresent()) {
      assertEquals(
          bigtableCluster.lastChange().get().toEpochMilli(),
          Timestamps.toMillis(clusterAutoscalerInfo.getLastChange()));
    } else {
      assertFalse(clusterAutoscalerInfo.hasLastChange());
    }

    if (bigtableCluster.lastFailure().isPresent()) {
      assertEquals(
          bigtableCluster.lastFailure().get().toEpochMilli(),
          Timestamps.toMillis(clusterAutoscalerInfo.getLastFailure()));
    } else {
      assertFalse(clusterAutoscalerInfo.hasLastFailure());
    }

    if (bigtableCluster.lastCheck().isPresent()) {
      assertEquals(
          bigtableCluster.lastCheck().get().toEpochMilli(),
          Timestamps.toMillis(clusterAutoscalerInfo.getLastCheck()));
    } else {
      assertFalse(clusterAutoscalerInfo.hasLastCheck());
    }

    if (bigtableCluster.lastFailureMessage().isPresent()) {
      assertEquals(
          bigtableCluster.lastFailureMessage().get(),
          clusterAutoscalerInfo.getLastFailureMessage().getValue());
    } else {
      assertFalse(clusterAutoscalerInfo.hasLastFailureMessage());
    }

    if (bigtableCluster.errorCode().isPresent()) {
      assertEquals(
          bigtableCluster.errorCode().get().name(),
          clusterAutoscalerInfo.getErrorCode().getValue());
    } else {
      assertFalse(clusterAutoscalerInfo.hasErrorCode());
    }

    final AutoscalerConfiguration configuration = clusterAutoscalerInfo.getConfiguration();
    assertEquals(bigtableCluster.projectId(), configuration.getCluster().getProjectId());
    assertEquals(bigtableCluster.instanceId(), configuration.getCluster().getInstanceId());
    assertEquals(bigtableCluster.clusterId(), configuration.getCluster().getClusterId());
    assertEquals(bigtableCluster.cpuTarget(), configuration.getCpuTarget(), 0.001);
    assertEquals(bigtableCluster.enabled(), configuration.getEnabled().getValue());
    assertEquals(bigtableCluster.minNodes(), configuration.getMinNodes());
    assertEquals(bigtableCluster.maxNodes(), configuration.getMaxNodes());
    assertEquals(
        bigtableCluster.minNodesOverride(), configuration.getMinNodesOverride().getValue());

    Optional<Integer> overloadStep =
        Optional.ofNullable(
            configuration.hasOverloadStep() ? configuration.getOverloadStep().getValue() : null);
    assertEquals(bigtableCluster.overloadStep(), overloadStep);
    assertEquals(
        bigtableCluster.consecutiveFailureCount(),
        clusterAutoscalerInfo.getConsecutiveFailureCount());
  }

  private void testEquivalence(
      final Iterable<ClusterAutoscalerInfo> clusterAutoscalerInfos,
      final Iterable<BigtableCluster> bigtableCluster) {

    for (BigtableCluster cluster : bigtableCluster) {
      for (ClusterAutoscalerInfo clusterAutoscalerInfo : clusterAutoscalerInfos) {
        final ClusterIdentifier clusterIdentifier =
            clusterAutoscalerInfo.getConfiguration().getCluster();
        if (cluster.projectId().equals(clusterIdentifier.getProjectId())
            && cluster.instanceId().equals(clusterIdentifier.getInstanceId())
            && cluster.clusterId().equals(clusterIdentifier.getClusterId())) {
          testEquivalent(clusterAutoscalerInfo, cluster);
          break;
        }
      }
    }
  }

  @Test
  public void testGetAllClusters() {
    final OptionalClusterIdentifier emptyIdentifier =
        OptionalClusterIdentifier.newBuilder().build();
    final ClusterAutoscalerInfoList clusterAutoscalerInfoList = blockingStub.get(emptyIdentifier);
    assertEquals(CLUSTERS.length, clusterAutoscalerInfoList.getClusterAutoscalerInfoCount());
    testEquivalence(
        clusterAutoscalerInfoList.getClusterAutoscalerInfoList(), Arrays.asList(CLUSTERS));
  }

  @Test
  public void testGetAllClustersForInstance() {
    final OptionalClusterIdentifier instanceIdentifier =
        OptionalClusterIdentifier.newBuilder()
            .setInstanceId(StringValue.of(ENABLED_CLUSTER.instanceId()))
            .build();
    final List<BigtableCluster> expectedClusters =
        Arrays.stream(CLUSTERS)
            .filter(c -> c.instanceId().equals(ENABLED_CLUSTER.instanceId()))
            .collect(Collectors.toList());

    final ClusterAutoscalerInfoList clusterAutoscalerInfoList =
        blockingStub.get(instanceIdentifier);
    assertEquals(
        expectedClusters.size(), clusterAutoscalerInfoList.getClusterAutoscalerInfoCount());
    testEquivalence(clusterAutoscalerInfoList.getClusterAutoscalerInfoList(), expectedClusters);
  }

  @Test
  public void testGetAllClustersForProject() {
    final OptionalClusterIdentifier projectIdentifier =
        OptionalClusterIdentifier.newBuilder()
            .setProjectId(StringValue.of(ENABLED_CLUSTER.projectId()))
            .build();
    final List<BigtableCluster> expectedClusters =
        Arrays.stream(CLUSTERS)
            .filter(c -> c.projectId().equals(ENABLED_CLUSTER.projectId()))
            .collect(Collectors.toList());

    final ClusterAutoscalerInfoList clusterAutoscalerInfoList = blockingStub.get(projectIdentifier);
    assertEquals(
        expectedClusters.size(), clusterAutoscalerInfoList.getClusterAutoscalerInfoCount());
    testEquivalence(clusterAutoscalerInfoList.getClusterAutoscalerInfoList(), expectedClusters);
  }

  @Test
  public void testGetAllClustersForProjectInstance() {
    final OptionalClusterIdentifier projectInstanceIdentifier =
        OptionalClusterIdentifier.newBuilder()
            .setProjectId(StringValue.of(ENABLED_CLUSTER.projectId()))
            .setInstanceId(StringValue.of(ENABLED_CLUSTER.instanceId()))
            .build();
    final List<BigtableCluster> expectedClusters =
        Arrays.stream(CLUSTERS)
            .filter(
                c ->
                    c.projectId().equals(ENABLED_CLUSTER.projectId())
                        && c.instanceId().equals(ENABLED_CLUSTER.instanceId()))
            .collect(Collectors.toList());

    final ClusterAutoscalerInfoList clusterAutoscalerInfoList =
        blockingStub.get(projectInstanceIdentifier);
    assertEquals(
        expectedClusters.size(), clusterAutoscalerInfoList.getClusterAutoscalerInfoCount());
    testEquivalence(clusterAutoscalerInfoList.getClusterAutoscalerInfoList(), expectedClusters);
  }

  @Test
  public void testGetSingleCluster() {
    final OptionalClusterIdentifier enabledClusterIdentifier =
        OptionalClusterIdentifier.newBuilder()
            .setProjectId(StringValue.of(ENABLED_CLUSTER.projectId()))
            .setInstanceId(StringValue.of(ENABLED_CLUSTER.instanceId()))
            .setClusterId(StringValue.of(ENABLED_CLUSTER.clusterId()))
            .build();

    final ClusterAutoscalerInfoList clusterAutoscalerInfoList =
        blockingStub.get(enabledClusterIdentifier);
    assertEquals(1, clusterAutoscalerInfoList.getClusterAutoscalerInfoCount());
    testEquivalence(
        clusterAutoscalerInfoList.getClusterAutoscalerInfoList(),
        Collections.singletonList(ENABLED_CLUSTER));
  }

  @Test
  public void testGetNonExistingCluster() {
    final OptionalClusterIdentifier nonExistingClusterIdentifier =
        OptionalClusterIdentifier.newBuilder()
            .setProjectId(StringValue.of(NONEXISTING_CLUSTER_IDENTIFIER.getProjectId()))
            .setInstanceId(StringValue.of(NONEXISTING_CLUSTER_IDENTIFIER.getInstanceId()))
            .setClusterId(StringValue.of(NONEXISTING_CLUSTER_IDENTIFIER.getClusterId()))
            .build();
    final ClusterAutoscalerInfoList clusterAutoscalerInfoList =
        blockingStub.get(nonExistingClusterIdentifier);
    assertEquals(0, clusterAutoscalerInfoList.getClusterAutoscalerInfoCount());
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

  @Test
  public void testDeleteExistingCluster() {
    final com.google.rpc.Status delete = blockingStub.delete(ENABLED_CLUSTER_IDENTIFIER);
    assertEquals(Status.OK.getCode().value(), delete.getCode());
    verify(db, times(1))
        .deleteBigtableCluster(
            ApiTestResources.ENABLED_CLUSTER.projectId(),
            ApiTestResources.ENABLED_CLUSTER.instanceId(),
            ApiTestResources.ENABLED_CLUSTER.clusterId());
  }

  @Test
  public void testDeleteNonExistingCluster() {
    final com.google.rpc.Status delete = blockingStub.delete(NONEXISTING_CLUSTER_IDENTIFIER);
    assertEquals(Status.NOT_FOUND.getCode().value(), delete.getCode());
    verify(db, times(1))
        .deleteBigtableCluster(
            NONEXISTING_CLUSTER_IDENTIFIER.getProjectId(),
            NONEXISTING_CLUSTER_IDENTIFIER.getInstanceId(),
            NONEXISTING_CLUSTER_IDENTIFIER.getClusterId());
  }

  @Test
  public void createClusterFail() {
    exception.expect(StatusRuntimeException.class);
    exception.expectMessage(SQL_EXCEPTION.toString());
    blockingStub.create(INVALID_CONFIGURATION);
    verify(db, times(1)).insertBigtableCluster(any());
  }

  @Test
  public void createClusterSuccessfully() {
    final AutoscalerConfiguration autoscalerConfiguration =
        MessageConverters.convertToAutoscalerConfiguration(ENABLED_CLUSTER);

    final com.google.rpc.Status status = blockingStub.create(autoscalerConfiguration);
    System.out.println(status);
    assertEquals(Status.OK.getCode().value(), status.getCode());
    verify(db, times(1)).insertBigtableCluster(any());
  }

  @Test
  public void updateClusterFail() {
    exception.expect(StatusRuntimeException.class);
    exception.expectMessage(SQL_EXCEPTION.toString());
    blockingStub.update(INVALID_CONFIGURATION);
    verify(db, times(1)).updateBigtableCluster(any());
  }

  @Test
  public void updateClusterSuccessfully() {
    final AutoscalerConfiguration autoscalerConfiguration =
        MessageConverters.convertToAutoscalerConfiguration(ENABLED_CLUSTER);

    final com.google.rpc.Status status = blockingStub.update(autoscalerConfiguration);
    System.out.println(status);
    assertEquals(Status.OK.getCode().value(), status.getCode());
    verify(db, times(1)).updateBigtableCluster(any());
  }
}
