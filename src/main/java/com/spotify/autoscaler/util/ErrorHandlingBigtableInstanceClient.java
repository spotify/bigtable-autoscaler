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

package com.spotify.autoscaler.util;

import com.google.bigtable.admin.v2.Cluster;
import com.google.bigtable.admin.v2.CreateInstanceRequest;
import com.google.bigtable.admin.v2.DeleteInstanceRequest;
import com.google.bigtable.admin.v2.GetClusterRequest;
import com.google.bigtable.admin.v2.Instance;
import com.google.bigtable.admin.v2.ListClustersRequest;
import com.google.bigtable.admin.v2.ListClustersResponse;
import com.google.bigtable.admin.v2.ListInstancesRequest;
import com.google.bigtable.admin.v2.ListInstancesResponse;
import com.google.cloud.bigtable.grpc.BigtableInstanceClient;
import com.google.longrunning.GetOperationRequest;
import com.google.longrunning.Operation;
import com.google.protobuf.Empty;
import com.spotify.autoscaler.db.Database;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorHandlingBigtableInstanceClient implements BigtableInstanceClient {

  private final BigtableInstanceClient underlyingClient;
  private final Database db;
  private static final Logger logger = LoggerFactory.getLogger(ErrorHandlingBigtableInstanceClient.class);

  private ErrorHandlingBigtableInstanceClient(BigtableInstanceClient client, Database db) {
    this.underlyingClient = client;
    this.db = db;
  }

  public static BigtableInstanceClient create(BigtableInstanceClient client, Database db){
    return new ErrorHandlingBigtableInstanceClient(client, db);
  }

  @Override
  public Operation createInstance(final CreateInstanceRequest request) {
      return underlyingClient.createInstance(request);
  }

  @Override
  public Operation getOperation(final GetOperationRequest request) {
    return underlyingClient.getOperation(request);
  }

  @Override
  public void waitForOperation(final Operation operation) throws TimeoutException, IOException {
    underlyingClient.waitForOperation(operation);
  }

  @Override
  public void waitForOperation(final Operation operation, final long timeout, final TimeUnit timeUnit)
      throws IOException, TimeoutException {
    underlyingClient.waitForOperation(operation, timeout, timeUnit);
  }

  @Override
  public ListInstancesResponse listInstances(final ListInstancesRequest request) {
    return underlyingClient.listInstances(request);
  }

  @Override
  public Instance updateInstance(final Instance instance) {
    return underlyingClient.updateInstance(instance);
  }

  @Override
  public Empty deleteInstance(final DeleteInstanceRequest request) {
    return underlyingClient.deleteInstance(request);
  }

  @Override
  public Cluster getCluster(final GetClusterRequest request) {
    try {
      return underlyingClient.getCluster(request);
    } catch (StatusRuntimeException e) {
      handle(e, request.getName());
      throw e;
    }
  }

  @Override
  public ListClustersResponse listCluster(final ListClustersRequest request) {
    return underlyingClient.listCluster(request);
  }

  @Override
  public Operation updateCluster(final Cluster cluster) {
    try {
      return underlyingClient.updateCluster(cluster);
    } catch (StatusRuntimeException e) {
      handle(e, cluster.getName());
      throw e;
    }
  }

  private void handle(final StatusRuntimeException e, final String clusterName) {
    final Status status = e.getStatus();
    switch (status.getCode()) {
      case NOT_FOUND:
        Cluster cluster = Cluster.newBuilder().setName(clusterName).build();
        final Cluster.State state = cluster.getState();
        Pattern pattern = Pattern.compile("projects/(.+)/instances/(.+)/clusters/(.+)");
        Matcher matcher = pattern.matcher(clusterName);
        if (state == Cluster.State.STATE_NOT_KNOWN && matcher.find()) {
          String projectId = matcher.group(1);
          String instanceId = matcher.group(2);
          String clusterId = matcher.group(3);
          db.deleteBigtableCluster(projectId, instanceId, clusterId);
          logger.warn("Disabled cluster because it does not exist anymore");
        }
        break;
      case RESOURCE_EXHAUSTED:
        //TODO(gizem): Update maxNodes? Notify owner?
        break;
      default:
        return;
    }
  }
}
