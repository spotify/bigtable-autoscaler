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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.spotify.autoscaler.api.type.ReconcileResponse;
import com.spotify.autoscaler.db.Database;
import io.kubernetes.client.openapi.JSON;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;
import org.mockito.Mock;

public class KCCAutoscalerControllerTest extends JerseyTest {

  @Mock private Database db;
  private final Gson gson = new JSON().getGson();
  private static final String PROJECT_ID = "bigtable-playground";
  private static final String INSTANCE_ID = "this-is-my-bigtable-instance";

  @Override
  protected Application configure() {
    initMocks(this);
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(new KCCAutoscalerController(db));
    return resourceConfig;
  }

  @Test
  public void reconcileApply() {
    when(db.getBigtableClusters(any(), any()))
        .thenAnswer(invocation -> Collections.nCopies(3, ApiTestResources.CLUSTER));
    reconcile("apply", 3);
    verify(db, times(3)).reconcileBigtableCluster(any());
    verify(db, times(1))
        .deleteBigtableClustersExcept(
            eq(PROJECT_ID), eq(INSTANCE_ID), argThat(set -> set.size() == 3));
    verify(db, times(1)).getBigtableClusters(PROJECT_ID, INSTANCE_ID);
  }

  @Test(expected = Exception.class)
  public void reconcileApplyFail() {
    reconcile("apply", 3);
  }

  @Test
  public void reconcileDelete() {
    // by default mock db will return empty for getBigtableClusters
    reconcile("delete", 0);
    verify(db, times(0)).reconcileBigtableCluster(any());
    verify(db, times(1))
        .deleteBigtableClustersExcept(
            eq(PROJECT_ID), eq(INSTANCE_ID), argThat(set -> set.size() == 0));
    verify(db, times(1)).getBigtableClusters(PROJECT_ID, INSTANCE_ID);
  }

  private void reconcile(final String name, final int desiredClusterCount) {
    final Response response =
        target("/kcc/reconcile")
            .request()
            .post(Entity.text(readFile(String.format("kcc/%s.json", name))));
    assertThat(response.getStatusInfo(), equalTo(Response.Status.OK));
    final ReconcileResponse reconcileResponse =
        gson.fromJson(response.readEntity(String.class), ReconcileResponse.class);
    final int clusterCount =
        reconcileResponse.getEnsure().getForObject().getStatus().getClusterCount();
    assertThat(clusterCount, equalTo(desiredClusterCount));
  }

  @SuppressWarnings("UnstableApiUsage")
  private String readFile(final String name) {
    try {
      return Resources.toString(Resources.getResource(name), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException((e));
    }
  }
}
