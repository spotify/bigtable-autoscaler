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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import com.spotify.autoscaler.util.ErrorCode;
import io.norberg.automatter.jackson.AutoMatterModule;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

public interface ApiTestResources {

  String INSTANCES = "/instances";
  String HEALTH = "/health";
  String LOAD = "/instances/load";
  String SERVICE_NAME = "test-service";

  BigtableCluster CLUSTER =
      new BigtableClusterBuilder()
          .clusterId("c")
          .projectId("p")
          .instanceId("i")
          .cpuTarget(0.5)
          .minNodes(3)
          .maxNodes(5)
          .enabled(true)
          .loadDelta(10)
          .errorCode(Optional.of(ErrorCode.OK))
          .build();

  ObjectMapper MAPPER =
      new ObjectMapper().registerModule(new AutoMatterModule()).registerModule(new Jdk8Module());

  default Invocation.Builder request(final WebTarget target, final BigtableCluster cluster) {
    return target
        .queryParam("projectId", cluster.projectId())
        .queryParam("instanceId", cluster.instanceId())
        .queryParam("clusterId", cluster.clusterId())
        .queryParam("cpuTarget", Double.toString(cluster.cpuTarget()))
        .queryParam("minNodes", Integer.toString(cluster.minNodes()))
        .queryParam("maxNodes", Integer.toString(cluster.maxNodes()))
        .queryParam("enabled", Boolean.toString(cluster.enabled()))
        .request();
  }

  default List<BigtableCluster> deserialize(final Response response) throws IOException {
    final CollectionType type =
        TypeFactory.defaultInstance().constructCollectionType(List.class, BigtableCluster.class);
    return MAPPER.readValue(response.readEntity(String.class), type);
  }
}
