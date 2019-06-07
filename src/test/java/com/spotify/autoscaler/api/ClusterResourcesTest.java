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
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.spotify.autoscaler.AutoscaleResourceConfig;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.Database;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;
import org.mockito.Mock;

public class ClusterResourcesTest extends JerseyTest implements ApiTestResources {

  @Mock private Database db;

  private boolean insertBigtableClusterResult;
  private boolean updateBigtableClusterResult;
  private boolean deleteBigtableClusterResult;
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

    Config config = ConfigFactory.load(SERVICE_NAME);
    ResourceConfig resourceConfig =
        new AutoscaleResourceConfig(
            SERVICE_NAME, config, new ClusterResources(db), new HealthCheck(db));

    return resourceConfig;
  }

  @Test
  public void getEmptyAllInstances() {
    getBigtableClustersResult = List.of();
    Response response = target(INSTANCES).request().get();
    assertThat(response.getStatusInfo(), equalTo(Response.Status.OK));
    assertThat(response.readEntity(String.class), equalTo("[]"));
  }

  @Test
  public void getNonEmptyAllInstances() throws IOException {
    getBigtableClustersResult = List.of(CLUSTER);
    Response response = target(INSTANCES).request().get();
    assertThat(response.getStatusInfo(), equalTo(Response.Status.OK));
    List<BigtableCluster> parsed = deserialize(response);
    assertThat(parsed.size(), equalTo(1));
    assertThat(parsed.get(0).clusterId(), equalTo("c"));
  }

  @Test
  public void createInstance() {
    insertBigtableClusterResult = true;
    Response response = request(target(INSTANCES), CLUSTER).post(Entity.text(""));
    assertThat(response.getStatusInfo(), equalTo(Response.Status.OK));
    assertThat(response.readEntity(String.class), equalTo(""));
  }

  @Test
  public void updateInstance() {
    updateBigtableClusterResult = true;
    Response response = request(target(INSTANCES), CLUSTER).put(Entity.text(""));
    assertThat(response.getStatusInfo(), equalTo(Response.Status.OK));
    assertThat(response.readEntity(String.class), equalTo(""));
  }

  @Test
  public void deleteInstance() {
    deleteBigtableClusterResult = true;
    Response response =
        target(INSTANCES)
            .queryParam("projectId", CLUSTER.projectId())
            .queryParam("instanceId", CLUSTER.instanceId())
            .queryParam("clusterId", CLUSTER.clusterId())
            .request()
            .delete();
    assertThat(response.getStatusInfo(), equalTo(Response.Status.OK));
    assertThat(response.readEntity(String.class), equalTo(""));
  }
}
