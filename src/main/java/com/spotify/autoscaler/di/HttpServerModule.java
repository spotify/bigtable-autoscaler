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

package com.spotify.autoscaler.di;

import static com.spotify.autoscaler.Main.SERVICE_NAME;

import com.spotify.autoscaler.AutoscaleResourceConfig;
import com.spotify.autoscaler.api.ClusterResources;
import com.spotify.autoscaler.api.HealthCheck;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.util.BigtableUtil;
import com.typesafe.config.Config;
import dagger.Module;
import dagger.Provides;
import java.net.URI;
import java.net.URISyntaxException;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Module
public class HttpServerModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerModule.class);

  @Provides
  public HttpServer initializeServer(final Config config, final Database database) {
    final ResourceConfig resourceConfig =
        new AutoscaleResourceConfig(
            SERVICE_NAME,
            config,
            new ClusterResources(
                database,
                cluster -> BigtableUtil.createSession(cluster.instanceId(), cluster.projectId())),
            new HealthCheck(database));
    final int port = config.getConfig("http").getConfig("server").getInt("port");
    try {
      return GrizzlyHttpServerFactory.createHttpServer(
          new URI("http://0.0.0.0:" + port), resourceConfig, false);
    } catch (URISyntaxException e) {
      LOGGER.error("Failed to initialize http server", e);
      throw new RuntimeException(e);
    }
  }
}
