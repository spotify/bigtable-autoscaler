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
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/kcc")
public class KCCAutoscalerController implements Endpoint {

  // Use the GSON provided by the k8s java client to deal with k8s objects, since
  // it implements several typeadapter to decode k8s object correctly.
  private final Gson gson = new JSON().getGson();

  private static final Logger LOGGER = LoggerFactory.getLogger(KCCAutoscalerController.class);

  private final Database database;

  @Inject
  public KCCAutoscalerController(final Database database) {
    this.database = checkNotNull(database);
  }

  @POST
  @Path("/reconcile")
  public String reconcile(String request) {

    final ReconcileRequest reconcileRequest = gson.fromJson(request, ReconcileRequest.class);
    final BigtableAutoscaler autoscalerConfig = reconcileRequest.getForObject();
    final ReconcileResponse.EnsureResources.ForObject forObj =
        new ReconcileResponse.EnsureResources.ForObject(autoscalerConfig.getApiVersion(), autoscalerConfig.getKind(),
            autoscalerConfig.getMetadata().getNamespace(), autoscalerConfig.getMetadata().getName());

    final String projectId = autoscalerConfig.getMetadata().getNamespace();
    final String instanceId = autoscalerConfig.getSpec().getInstanceId();

    if (reconcileRequest.getDeletionInProgress()) {
      int deletedClustersCount = database.deleteBigtableClusters(projectId, instanceId);
      LOGGER.info("Project {}, instance {}: {} clusters deleted", projectId, instanceId, deletedClustersCount);
      forObj.deleted(true);
      return gson.toJson(new ReconcileResponse().ensure(new ReconcileResponse.EnsureResources().forObject(forObj)),
          ReconcileResponse.class);
    }

    final Map<String, BigtableAutoscaler.Spec.Cluster> targetClusters =
        Arrays.stream(autoscalerConfig.getSpec().getCluster()).collect(Collectors.toMap(
            BigtableAutoscaler.Spec.Cluster::getClusterId, Function.identity()));

    targetClusters.forEach(
        (clusterId, clusterSpec) ->
            database.reconcileBigtableCluster(
                new BigtableClusterBuilder()
                    .projectId(projectId)
                    .instanceId(instanceId)
                    .clusterId(clusterId)
                    .minNodes(clusterSpec.getMinNodes())
                    .maxNodes(clusterSpec.getMaxNodes())
                    .cpuTarget(clusterSpec.getCpuTarget())
                    .enabled(true)
                    .build()));

    int deletedClustersCount = database.deleteBigtableClustersExcept(projectId, instanceId, targetClusters.keySet());
    if (deletedClustersCount > 0) {
      LOGGER.info("Project {}, instance {}: {} clusters deleted", projectId, instanceId, deletedClustersCount);
    }

    final List<BigtableCluster> clusters = database.getBigtableClusters(projectId, instanceId);
    forObj.status(gson.toJsonTree(new ReconcileResponse.Status(clusters), ReconcileResponse.Status.class));

    return gson.toJson(new ReconcileResponse().ensure(new ReconcileResponse.EnsureResources().forObject(forObj)),
        ReconcileResponse.class);
  }

}
