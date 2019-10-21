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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.di.HttpServerModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;
import org.mockito.Mock;

public class ClusterResourcesTest extends JerseyTest implements ApiTestResources {

  @Mock private Database db;

  private boolean insertBigtableClusterResult;
  private boolean updateBigtableClusterResult;
  private boolean deleteBigtableClusterResult;
  private boolean minNodesOverrideResult;
  private Collection<BigtableCluster> getBigtableClustersResult;

  @Override
  protected Application configure() {
    initMocks(this);
    when(db.insertBigtableCluster(any())).thenAnswer(invocation -> insertBigtableClusterResult);
    when(db.updateBigtableCluster(any())).thenAnswer(invocation -> updateBigtableClusterResult);
    when(db.deleteBigtableCluster(any(), any(), any()))
        .thenAnswer(invocation -> deleteBigtableClusterResult);
    when(db.getBigtableClusters(any(), any(), any()))
        .thenAnswer(invocation -> getBigtableClustersResult);
    when(db.getBigtableCluster(any(), any(), any()))
        .thenAnswer(invocation -> Optional.of(ApiTestResources.ENABLED_CLUSTER));
    when(db.setMinNodesOverride(any(), any(), any(), any()))
        .thenAnswer(invocation -> minNodesOverrideResult);

    final Config config = ConfigFactory.load(ApiTestResources.SERVICE_NAME);

    return new HttpServerModule()
        .resourceConfig(
            config,
            ImmutableSet.of(
                new ClusterResources(db, ApiTestResources.MAPPER), new HealthCheck(db)));
  }

  @Test
  public void getEmptyAllClusters() {
    getBigtableClustersResult = ImmutableList.of();
    final Response response = target(ApiTestResources.CLUSTERS_ENDPOINT).request().get();
    assertThat(response.getStatusInfo(), equalTo(Response.Status.OK));
    assertThat(response.readEntity(String.class), equalTo("[]"));
  }

  @Test
  public void getNonEmptyAllClusters() throws IOException {
    getBigtableClustersResult = ImmutableList.of(ApiTestResources.ENABLED_CLUSTER);
    final Response response = target(ApiTestResources.CLUSTERS_ENDPOINT).request().get();
    assertThat(response.getStatusInfo(), equalTo(Response.Status.OK));
    final List<BigtableCluster> parsed = deserialize(response);
    assertThat(parsed.size(), equalTo(1));
    assertThat(parsed.get(0).clusterId(), equalTo("c"));
  }

  @Test
  public void createCluster() {
    insertBigtableClusterResult = true;
    final Response response =
        request(target(ApiTestResources.CLUSTERS_ENDPOINT), ApiTestResources.ENABLED_CLUSTER)
            .post(Entity.text(""));
    assertThat(response.getStatusInfo(), equalTo(Response.Status.OK));
    assertThat(response.readEntity(String.class), equalTo(""));
    verify(db, times(1)).insertBigtableCluster(any());
  }

  @Test
  public void updateCluster() {
    updateBigtableClusterResult = true;
    final Response response =
        request(target(ApiTestResources.CLUSTERS_ENDPOINT), ApiTestResources.ENABLED_CLUSTER)
            .put(Entity.text(""));
    assertThat(response.getStatusInfo(), equalTo(Response.Status.OK));
    assertThat(response.readEntity(String.class), equalTo(""));
    verify(db, times(1)).updateBigtableCluster(any());
  }

  @Test
  public void deleteCluster() {
    deleteBigtableClusterResult = true;
    final Response response =
        target(ApiTestResources.CLUSTERS_ENDPOINT)
            .queryParam("projectId", ApiTestResources.ENABLED_CLUSTER.projectId())
            .queryParam("instanceId", ApiTestResources.ENABLED_CLUSTER.instanceId())
            .queryParam("clusterId", ApiTestResources.ENABLED_CLUSTER.clusterId())
            .request()
            .delete();
    assertThat(response.getStatusInfo(), equalTo(Response.Status.OK));
    assertThat(response.readEntity(String.class), equalTo(""));
    verify(db, times(1))
        .deleteBigtableCluster(
            ApiTestResources.ENABLED_CLUSTER.projectId(),
            ApiTestResources.ENABLED_CLUSTER.instanceId(),
            ApiTestResources.ENABLED_CLUSTER.clusterId());
  }

  @Test
  public void extraLoad() {
    minNodesOverrideResult = true;
    final Response response =
        target(ApiTestResources.LOAD_ENDPOINT)
            .queryParam("projectId", ApiTestResources.ENABLED_CLUSTER.projectId())
            .queryParam("instanceId", ApiTestResources.ENABLED_CLUSTER.instanceId())
            .queryParam("clusterId", ApiTestResources.ENABLED_CLUSTER.clusterId())
            .queryParam("minNodesOverride", ApiTestResources.ENABLED_CLUSTER.minNodesOverride())
            .request()
            .put(Entity.text(""));
    assertThat(response.getStatusInfo(), equalTo(Response.Status.OK));
    assertThat(response.readEntity(String.class), equalTo(""));
    verify(db, times(1))
        .setMinNodesOverride(
            ApiTestResources.ENABLED_CLUSTER.projectId(),
            ApiTestResources.ENABLED_CLUSTER.instanceId(),
            ApiTestResources.ENABLED_CLUSTER.clusterId(),
            ApiTestResources.ENABLED_CLUSTER.minNodesOverride());
  }

  @Test
  public void testMinNodesOverrideIsGreaterThanMaxNodes() {
    minNodesOverrideResult = true;
    final Response response =
        target(ApiTestResources.LOAD_ENDPOINT)
            .queryParam("projectId", ApiTestResources.ENABLED_CLUSTER.projectId())
            .queryParam("instanceId", ApiTestResources.ENABLED_CLUSTER.instanceId())
            .queryParam("clusterId", ApiTestResources.ENABLED_CLUSTER.clusterId())
            .queryParam("minNodesOverride", 1000)
            .request()
            .put(Entity.text(""));
    assertThat(response.getStatusInfo(), equalTo(Response.Status.OK));
    verify(db, times(1))
        .setMinNodesOverride(
            ApiTestResources.ENABLED_CLUSTER.projectId(),
            ApiTestResources.ENABLED_CLUSTER.instanceId(),
            ApiTestResources.ENABLED_CLUSTER.clusterId(),
            1000);
  }

  @Test
  public void testFailureIfClusterIfMissing() {
    minNodesOverrideResult = true;
    when(db.getBigtableCluster(any(), any(), any())).thenAnswer(invocation -> Optional.empty());
    final Response response =
        target(ApiTestResources.LOAD_ENDPOINT)
            .queryParam("projectId", ApiTestResources.ENABLED_CLUSTER.projectId())
            .queryParam("instanceId", ApiTestResources.ENABLED_CLUSTER.instanceId())
            .queryParam("clusterId", ApiTestResources.ENABLED_CLUSTER.clusterId())
            .queryParam("minNodesOverride", ApiTestResources.ENABLED_CLUSTER.minNodesOverride())
            .request()
            .put(Entity.text(""));
    assertThat(response.getStatusInfo(), equalTo(Response.Status.NOT_FOUND));
    verify(db, never()).setMinNodesOverride(any(), any(), any(), any());
  }
}
