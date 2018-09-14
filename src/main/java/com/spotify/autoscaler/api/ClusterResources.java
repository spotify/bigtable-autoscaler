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
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.util.BigtableUtil;
import io.norberg.automatter.jackson.AutoMatterModule;
import java.util.Optional;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Support two different paths for the moment while switching from instances->clusters
@Path("/{ignored:instances|clusters}")
@Produces(MediaType.APPLICATION_JSON)
public class ClusterResources {

  private static final Logger logger = LoggerFactory.getLogger(ClusterResources.class);

  private final Database db;
  private static final ObjectMapper mapper = new ObjectMapper()
      .registerModule(new AutoMatterModule())
      .registerModule(new Jdk8Module());

  public ClusterResources(Database db) {
    this.db = checkNotNull(db);
  }

  @GET
  public Response getAllClusters() {
    try {
      return Response.ok(mapper.writeValueAsString(db.getBigtableClusters())).build();
    } catch (JsonProcessingException e) {
      return Response.serverError().build();
    }
  }

  @GET
  @Path("logs")
  public Response getLogs(@QueryParam("projectId") String projectId,
                          @QueryParam("instanceId") String instanceId,
                          @QueryParam("clusterId") String clusterId) {
    try {
      return Response.ok(mapper.writeValueAsString(db.getLatestResizeEvents(projectId, instanceId, clusterId))).build();
    } catch (JsonProcessingException e) {
      return Response.serverError().build();
    }
  }

  @POST
  public Response createCluster(@QueryParam("projectId") String projectId,
                                @QueryParam("instanceId") String instanceId,
                                @QueryParam("clusterId") String clusterId,
                                @QueryParam("minNodes") Integer minNodes,
                                @QueryParam("maxNodes") Integer maxNodes,
                                @QueryParam("cpuTarget") Double cpuTarget,
                                @QueryParam("overloadStep") Integer overloadStep,
                                @QueryParam("enabled") @DefaultValue("true") Boolean enabled,
                                @QueryParam("loadDelta") @DefaultValue("0") Integer loadDelta) {
    BigtableCluster cluster = new BigtableClusterBuilder()
        .projectId(projectId)
        .instanceId(instanceId)
        .clusterId(clusterId)
        .minNodes(minNodes)
        .maxNodes(maxNodes)
        .cpuTarget(cpuTarget)
        .overloadStep(Optional.ofNullable(overloadStep))
        .enabled(enabled)
        .loadDelta(loadDelta)
        .build();
    try {
      BigtableUtil.pushContext(cluster);
      if (db.insertBigtableCluster(cluster)) {
        logger.info(String.format("cluster created: %s", cluster.toString()));
        return Response.ok().build();
      } else {
        return Response.serverError().build();
      }
    } finally {
      BigtableUtil.clearContext();
    }
  }

  @PUT
  public Response updateCluster(@QueryParam("projectId") String projectId,
                                @QueryParam("instanceId") String instanceId,
                                @QueryParam("clusterId") String clusterId,
                                @QueryParam("minNodes") Integer minNodes,
                                @QueryParam("maxNodes") Integer maxNodes,
                                @QueryParam("cpuTarget") Double cpuTarget,
                                @QueryParam("overloadStep") Integer overloadStep,
                                @QueryParam("enabled") @DefaultValue("true") Boolean enabled) {
    BigtableCluster cluster = new BigtableClusterBuilder()
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
      BigtableUtil.pushContext(cluster);
      if (db.updateBigtableCluster(cluster)) {
        logger.info(String.format("cluster updated: %s", cluster.toString()));
        return Response.ok().build();
      } else {
        return Response.serverError().build();
      }
    } finally {
      BigtableUtil.clearContext();
    }
  }

  @DELETE
  public Response deleteCluster(@QueryParam("projectId") String projectId,
                                @QueryParam("instanceId") String instanceId,
                                @QueryParam("clusterId") String clusterId) {
    BigtableCluster cluster = new BigtableClusterBuilder()
        .projectId(projectId)
        .instanceId(instanceId)
        .clusterId(clusterId)
        .minNodes(0)
        .maxNodes(0)
        .cpuTarget(0)
        .overloadStep(Optional.ofNullable(0))
        .enabled(true)
        .loadDelta(0)
        .build();
    try {
      BigtableUtil.pushContext(cluster);
      if (db.deleteBigtableCluster(projectId, instanceId, clusterId)) {
        logger.info(String.format("cluster deleted: %s/%s/%s", projectId, instanceId, clusterId));
        return Response.ok().build();
      } else {
        return Response.serverError().build();
      }
    } finally {
      BigtableUtil.clearContext();
    }
  }

  @PUT
  @Path("load")
  @Produces(MediaType.APPLICATION_JSON)
  public Response setExtraLoad(@QueryParam("projectId") String projectId,
                          @QueryParam("instanceId") String instanceId,
                          @QueryParam("clusterId") String clusterId,
                          @QueryParam("loadDelta") Integer loadDelta) {
    BigtableCluster cluster = new BigtableClusterBuilder()
        .projectId(projectId)
        .instanceId(instanceId)
        .clusterId(clusterId)
        .minNodes(0)
        .maxNodes(0)
        .cpuTarget(0)
        .overloadStep(Optional.ofNullable(0))
        .enabled(true)
        .loadDelta(loadDelta)
        .build();
    try {
      BigtableUtil.pushContext(cluster);
      if (db.updateLoadDelta(projectId, instanceId, clusterId, loadDelta)) {
        logger.info("cluster loadDelta updated to {}", loadDelta);
        return Response.ok().build();
      } else {
        return Response.serverError().build();
      }
    } finally {
      BigtableUtil.clearContext();
    }
  }

}
