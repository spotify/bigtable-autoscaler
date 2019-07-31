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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.autoscaler.LoggerContext;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.Database;
import java.util.Optional;
import javax.inject.Inject;
import javax.validation.constraints.Size;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Support two different paths for the moment while switching from instances->clusters
@Path("/{ignored:instances|clusters}")
@Produces(MediaType.APPLICATION_JSON)
public class ClusterResources implements Endpoint {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterResources.class);

  private final Database database;
  private final ObjectMapper mapper;

  @Inject
  public ClusterResources(final Database database, final ObjectMapper mapper) {
    this.database = checkNotNull(database);
    this.mapper = mapper;
  }

  @GET
  public Response getAllClusters(
      @QueryParam("projectId") @Size(min = 1) final String projectId,
      @QueryParam("instanceId") @Size(min = 1) final String instanceId,
      @QueryParam("clusterId") @Size(min = 1) final String clusterId) {
    try {
      return Response.ok(
              mapper.writeValueAsString(
                  database.getBigtableClusters(projectId, instanceId, clusterId)))
          .build();
    } catch (final JsonProcessingException e) {
      return Response.serverError().build();
    }
  }

  @GET
  @Path("enabled")
  public Response enabled(
      @QueryParam("projectId") @Size(min = 1) final String projectId,
      @QueryParam("instanceId") @Size(min = 1) final String instanceId,
      @QueryParam("clusterId") @Size(min = 1) final String clusterId) {
    final Optional<BigtableCluster> cluster =
        database.getBigtableCluster(projectId, instanceId, clusterId);
    try {
      if (cluster.isPresent()) {
        return Response.ok(mapper.writeValueAsString(cluster.get().enabled())).build();
      } else {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
    } catch (final JsonProcessingException e) {
      return Response.serverError().build();
    }
  }

  @GET
  @Path("logs")
  public Response getLogs(
      @QueryParam("projectId") @Size(min = 1) final String projectId,
      @QueryParam("instanceId") @Size(min = 1) final String instanceId,
      @QueryParam("clusterId") @Size(min = 1) final String clusterId) {
    try {
      return Response.ok(
              mapper.writeValueAsString(
                  database.getLatestResizeEvents(projectId, instanceId, clusterId)))
          .build();
    } catch (final JsonProcessingException e) {
      return Response.serverError().build();
    }
  }

  @POST
  public Response createCluster(
      @QueryParam("projectId") @Size(min = 1) final String projectId,
      @QueryParam("instanceId") @Size(min = 1) final String instanceId,
      @QueryParam("clusterId") @Size(min = 1) final String clusterId,
      @QueryParam("minNodes") final Integer minNodes,
      @QueryParam("maxNodes") final Integer maxNodes,
      @QueryParam("cpuTarget") final Double cpuTarget,
      @QueryParam("overloadStep") final Integer overloadStep,
      @QueryParam("enabled") @DefaultValue("true") final Boolean enabled,
      @QueryParam("minNodesOverride") @DefaultValue("0") final Integer minNodesOverride) {
    final BigtableCluster cluster =
        new BigtableClusterBuilder()
            .projectId(projectId)
            .instanceId(instanceId)
            .clusterId(clusterId)
            .minNodes(minNodes)
            .maxNodes(maxNodes)
            .cpuTarget(cpuTarget)
            .overloadStep(Optional.ofNullable(overloadStep))
            .enabled(enabled)
            .minNodesOverride(minNodesOverride)
            .build();
    try {
      LoggerContext.pushContext(cluster);
      if (database.insertBigtableCluster(cluster)) {
        LOGGER.info(String.format("cluster created: %s", cluster.toString()));
        return Response.ok().build();
      } else {
        return Response.serverError().build();
      }
    } finally {
      LoggerContext.clearContext();
    }
  }

  @PUT
  public Response updateCluster(
      @QueryParam("projectId") @Size(min = 1) final String projectId,
      @QueryParam("instanceId") @Size(min = 1) final String instanceId,
      @QueryParam("clusterId") @Size(min = 1) final String clusterId,
      @QueryParam("minNodes") final Integer minNodes,
      @QueryParam("maxNodes") final Integer maxNodes,
      @QueryParam("cpuTarget") final Double cpuTarget,
      @QueryParam("overloadStep") final Integer overloadStep,
      @QueryParam("enabled") @DefaultValue("true") final Boolean enabled) {
    final BigtableCluster cluster =
        new BigtableClusterBuilder()
            .projectId(projectId)
            .instanceId(instanceId)
            .clusterId(clusterId)
            .minNodes(minNodes)
            .maxNodes(maxNodes)
            .cpuTarget(cpuTarget)
            .overloadStep(Optional.ofNullable(overloadStep))
            .enabled(enabled)
            .build();
    try {
      LoggerContext.pushContext(cluster);
      if (database.updateBigtableCluster(cluster)) {
        LOGGER.info(String.format("cluster updated: %s", cluster.toString()));
        return Response.ok().build();
      } else {
        return Response.serverError().build();
      }
    } finally {
      LoggerContext.clearContext();
    }
  }

  @DELETE
  public Response deleteCluster(
      @QueryParam("projectId") @Size(min = 1) final String projectId,
      @QueryParam("instanceId") @Size(min = 1) final String instanceId,
      @QueryParam("clusterId") @Size(min = 1) final String clusterId) {
    final BigtableCluster cluster =
        new BigtableClusterBuilder()
            .projectId(projectId)
            .instanceId(instanceId)
            .clusterId(clusterId)
            .minNodes(0)
            .maxNodes(0)
            .cpuTarget(0)
            .overloadStep(Optional.of(0))
            .enabled(true)
            .minNodesOverride(0)
            .build();
    try {
      LoggerContext.pushContext(cluster);
      if (database.deleteBigtableCluster(projectId, instanceId, clusterId)) {
        LOGGER.info(String.format("cluster deleted: %s/%s/%s", projectId, instanceId, clusterId));
        return Response.ok().build();
      } else {
        return Response.serverError().build();
      }
    } finally {
      LoggerContext.clearContext();
    }
  }

  @PUT
  @Path("override-min-nodes")
  public Response setExtraLoad(
      @QueryParam("projectId") @Size(min = 1) final String projectId,
      @QueryParam("instanceId") @Size(min = 1) final String instanceId,
      @QueryParam("clusterId") @Size(min = 1) final String clusterId,
      @QueryParam("minNodesOverride") final Integer minNodesOverride) {

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
      if (!maybeCluster.isPresent()) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      if (database.setMinNodesOverride(projectId, instanceId, clusterId, minNodesOverride)) {
        LOGGER.info("cluster minNodesOverride updated to {}", minNodesOverride);
        return Response.ok().build();
      } else {
        return Response.serverError().build();
      }
    } finally {
      LoggerContext.clearContext();
    }
  }
}
