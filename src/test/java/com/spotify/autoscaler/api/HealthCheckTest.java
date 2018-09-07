/*-
 * -\-\-
 * bigtable-autoscaler
 * --
 * Copyright (C) 2018 Spotify AB
 * --
 * The Apache Software License, Version 2.0
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

import com.spotify.autoscaler.AutoscaleResourceConfig;
import com.spotify.autoscaler.db.Database;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;
import org.mockito.Mock;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.MockitoAnnotations.initMocks;

public class HealthCheckTest extends JerseyTest implements ApiTestResources {

  @Mock
  private Database db;

  private Runnable healthCheck;

  @Override
  protected Application configure() {
    initMocks(this);
    doAnswer(invocationOnMock -> {
      healthCheck.run(); return null;
    }).when(db).healthCheck();

    Config config = ConfigFactory.load(SERVICE_NAME);
    ResourceConfig resourceConfig =
        new AutoscaleResourceConfig(SERVICE_NAME, config, new ClusterResources(db), new HealthCheck(db));
    return resourceConfig;
  }

  @Test
  public void getOk() {
    healthCheck = () -> { };
    Response response = target(HEALTH).request().get();
    assertThat(response.getStatusInfo(), equalTo(Response.Status.OK));
    assertThat(response.readEntity(String.class), equalTo(""));
  }

  @Test
  public void getError() {
    healthCheck = () -> {
      throw new RuntimeException("Some db error");
    };
    Response response = target(HEALTH).request().get();
    assertThat(response.getStatusInfo(), equalTo(Response.Status.INTERNAL_SERVER_ERROR));
    assertThat(response.readEntity(String.class), equalTo("java.lang.RuntimeException: Some db error"));
  }
}
