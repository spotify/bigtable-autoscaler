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

import static com.google.api.client.util.Preconditions.checkNotNull;

import com.google.gson.Gson;
import com.spotify.autoscaler.api.type.BigtableAutoscaler;
import com.spotify.autoscaler.api.type.ReconcileRequest;
import com.spotify.autoscaler.api.type.ReconcileResponse;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.Database;
import io.kubernetes.client.openapi.JSON;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/kcc")
public class KCCAutoscalerController implements Endpoint {

  // Use the GSON provided by the k8s java client to deal with k8s objects, since
  // it implements several typeadapter to decode k8s object correctly.
  private static final Gson gson = new JSON().getGson();
  private static final Logger LOGGER = LoggerFactory.getLogger(KCCAutoscalerController.class);

  private final Database database;

  @Inject
  public KCCAutoscalerController(final Database database) {
    this.database = checkNotNull(database);
  }

  @POST
  @Path("/reconcile")
  @Produces("application/json")
  @Consumes("application/json")
  public ReconcileResponse reconcile(ReconcileRequest request) {

    final BigtableAutoscaler autoscalerConfig = request.getForObject();
    final ReconcileResponse.EnsureResources.ForObject forObj =
        new ReconcileResponse.EnsureResources.ForObject(autoscalerConfig.getApiVersion(), autoscalerConfig.getKind(),
        autoscalerConfig.getMetadata().getNamespace(), autoscalerConfig.getMetadata().getName());

    final String projectId = autoscalerConfig.getMetadata().getNamespace();
    final String instanceId = autoscalerConfig.getSpec().getInstanceId();
    final List<BigtableCluster> existingClustersList =
        database.getBigtableClusters(projectId, instanceId, null);

    if(request.getDeletionInProgress()) {
      existingClustersList.forEach(cluster -> database.deleteBigtableCluster(projectId, instanceId, cluster.clusterId()));
      forObj.deleted(true);
      return new ReconcileResponse().ensure(new ReconcileResponse.EnsureResources().forObject(forObj));
    }

    final Map<String, BigtableCluster> existingClusters =
        existingClustersList.stream().collect(Collectors.toMap(
            BigtableCluster::clusterId,
            Function.identity()));

    final Map<String, BigtableAutoscaler.Spec.Cluster> targetClusters =
        Arrays.stream(autoscalerConfig.getSpec().getCluster()).collect(Collectors.toMap(
            BigtableAutoscaler.Spec.Cluster::getClusterId, Function.identity()));

    final List<String> clustersToBeDeleted =
        existingClusters.keySet().stream().filter(clusterId -> !targetClusters.containsKey(clusterId))
            .collect(Collectors.toList());

    final List<String> clustersToBeCreated =
        targetClusters.keySet().stream().filter(clusterId -> !existingClusters.containsKey(clusterId))
            .collect(Collectors.toList());

    final List<String> clustersToBeModified =
        existingClusters.entrySet().stream().filter(entry -> targetClusters.containsKey(entry.getKey()))
            .filter(entry -> equivalent(entry.getValue(), targetClusters.get(entry.getKey())))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

    clustersToBeDeleted.forEach(clusterId -> database.deleteBigtableCluster(projectId, instanceId, clusterId));

    clustersToBeCreated.forEach(clusterId -> {
      final BigtableAutoscaler.Spec.Cluster cluster = targetClusters.get(clusterId);
      database.insertBigtableCluster(new BigtableClusterBuilder()
          .projectId(projectId)
          .instanceId(instanceId)
          .clusterId(clusterId)
          .minNodes(cluster.getMinNodes())
          .maxNodes(cluster.getMaxNodes())
          .cpuTarget(cluster.getCpuTarget())
          .storageTarget(0.7)
          .overloadStep(Optional.empty())
          .enabled(true)
          .extraEnabledAlgorithms(Optional.empty())
          .minNodesOverride(0)
          .build());
    });

    clustersToBeModified.forEach(clusterId -> {
      final BigtableAutoscaler.Spec.Cluster cluster = targetClusters.get(clusterId);
      final BigtableCluster existingCluster = existingClusters.get(clusterId);
      database.updateBigtableCluster(new BigtableClusterBuilder()
          .projectId(projectId)
          .instanceId(instanceId)
          .clusterId(clusterId)
          .minNodes(cluster.getMinNodes())
          .maxNodes(cluster.getMaxNodes())
          .cpuTarget(cluster.getCpuTarget())
          .storageTarget(existingCluster.storageTarget())
          .overloadStep(existingCluster.overloadStep())
          .enabled(existingCluster.enabled())
          .extraEnabledAlgorithms(existingCluster.extraEnabledAlgorithms())
          .build());
    });

    return new ReconcileResponse().ensure(new ReconcileResponse.EnsureResources().forObject(forObj));
  }

  private boolean equivalent(final BigtableCluster bigtableClusterInternal,
                             final BigtableAutoscaler.Spec.Cluster bigtableClusterK8s) {
    return bigtableClusterInternal.minNodes() == bigtableClusterK8s.getMinNodes()
           && bigtableClusterInternal.maxNodes() == bigtableClusterK8s.getMaxNodes()
           && bigtableClusterInternal.cpuTarget() == bigtableClusterK8s.getCpuTarget();
  }
}
