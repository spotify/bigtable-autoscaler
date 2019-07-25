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

package com.spotify.autoscaler;

import com.spotify.autoscaler.api.ClusterResources;
import com.spotify.autoscaler.api.HealthCheck;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.db.PostgresDatabase;
import com.spotify.autoscaler.filters.AllowAllClusterFilter;
import com.spotify.autoscaler.filters.ClusterFilter;
import com.spotify.autoscaler.metric.AutoscalerMetrics;
import com.spotify.autoscaler.util.BigtableUtil;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.metrics.ffwd.FastForwardReporter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/** Application entry point. */
public final class Main {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  public static final String SERVICE_NAME = "bigtable-autoscaler";
  public static final MetricId APP_PREFIX = MetricId.build("key", SERVICE_NAME);

  private static final Duration RUN_INTERVAL = Duration.ofSeconds(5);
  private static final int CONCURRENCY_LIMIT = 5;

  private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
  private final Autoscaler autoscaler;
  private final Database database;
  private final HttpServer server;
  private final FastForwardReporter reporter;
  private final StackdriverClient stackdriverClient = new StackdriverClient();

  /**
   * Runs the application.
   *
   * @param args command-line arguments
   */
  public static void main(final String... args) throws Exception {
    new Main();
  }

  private Main() throws URISyntaxException, IOException {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
    final Config config = ConfigFactory.load(SERVICE_NAME);

    final SemanticMetricRegistry registry = new SemanticMetricRegistry();
    final String ffwdHost = config.getString("ffwd.host");
    final int ffwdPort = config.getInt("ffwd.port");

    if (!ffwdHost.isEmpty()) {
      LOGGER.info("Connecting to ffwd at {}:{}", ffwdHost, ffwdPort);
      reporter =
          FastForwardReporter.forRegistry(registry)
              .prefix(APP_PREFIX)
              .host(ffwdHost)
              .port(ffwdPort)
              .schedule(TimeUnit.SECONDS, 5)
              .build();
      reporter.start();
    } else {
      reporter = null;
    }

    final int port = config.getConfig("http").getConfig("server").getInt("port");
    database = new PostgresDatabase(config.getConfig("database"));
    final URI uri = new URI("http://0.0.0.0:" + port);
    final ResourceConfig resourceConfig =
        new AutoscaleResourceConfig(
            SERVICE_NAME,
            config,
            new ClusterResources(
                database,
                cluster -> BigtableUtil.createSession(cluster.instanceId(), cluster.projectId())),
            new HealthCheck(database));
    server = GrizzlyHttpServerFactory.createHttpServer(uri, resourceConfig, false);

    ClusterFilter clusterFilter = new AllowAllClusterFilter();
    final String clusterFilterClass = config.getString("clusterFilter");
    if (clusterFilterClass != null && !clusterFilterClass.isEmpty()) {
      try {
        clusterFilter =
            (ClusterFilter)
                Class.forName(clusterFilterClass).getDeclaredConstructor().newInstance();
      } catch (final ClassNotFoundException
          | InstantiationException
          | IllegalAccessException
          | NoSuchMethodException
          | InvocationTargetException e) {
        LOGGER.error("Failed to create new instance of cluster filter " + clusterFilterClass, e);
      }
    }
    final AutoscalerMetrics autoscalerMetrics = new AutoscalerMetrics(registry);
    configureMetrics(autoscalerMetrics, database);
    autoscaler =
        new Autoscaler(
            Executors.newFixedThreadPool(CONCURRENCY_LIMIT),
            stackdriverClient,
            database,
            autoscalerMetrics,
            clusterFilter);

    executor.scheduleWithFixedDelay(
        autoscaler, RUN_INTERVAL.toMillis(), RUN_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    onShutdown();
                  } catch (final InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                  }
                }));
    server.start();
  }

  private void configureMetrics(AutoscalerMetrics autoscalerMetrics, Database database) {
    autoscalerMetrics.registerActiveClusters(database);
    autoscalerMetrics.registerOpenFileDescriptors();
    autoscalerMetrics.registerDailyResizeCount(database);
    autoscalerMetrics.registerFailureCount(database);
    autoscalerMetrics.registerOpenDatabaseConnections(database);
    autoscalerMetrics.scheduleCleanup(database);
  }

  private void onShutdown() throws ExecutionException, InterruptedException {
    try {
      stackdriverClient.close();
    } catch (final Exception e) {
      LOGGER.error("Exception while closing stackdriverClient", e);
    }
    server.shutdown(10, TimeUnit.SECONDS).get();
    if (reporter != null) {
      reporter.stop();
    }

    executor.shutdown();
    try {
      executor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      LOGGER.error("Exception while awaiting executor termination", e);
    }
    LOGGER.info("ScheduledExecutor stopped");
    autoscaler.close();
    LOGGER.info("Bigtable sessions and Stackdriver sessions have been closed");
    try {
      database.close();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    LOGGER.info("Database connection closed");
  }
}
