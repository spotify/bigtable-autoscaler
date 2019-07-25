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

import com.codahale.metrics.Gauge;
import com.spotify.autoscaler.api.ClusterResources;
import com.spotify.autoscaler.api.HealthCheck;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.db.PostgresDatabase;
import com.spotify.autoscaler.filters.AllowAllClusterFilter;
import com.spotify.autoscaler.filters.ClusterFilter;
import com.spotify.autoscaler.metric.AutoscalerMetrics;
import com.spotify.autoscaler.util.BigtableUtil;
import com.spotify.autoscaler.util.ErrorCode;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.metrics.ffwd.FastForwardReporter;
import com.sun.management.UnixOperatingSystemMXBean;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.lang.management.ManagementFactory;
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

  private static final Logger logger = LoggerFactory.getLogger(Main.class);
  public static final String SERVICE_NAME = "bigtable-autoscaler";
  public static final MetricId APP_PREFIX = MetricId.build("key", SERVICE_NAME);

  private static final Duration RUN_INTERVAL = Duration.ofSeconds(5);
  private static final int CONCURRENCY_LIMIT = 5;

  private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
  private final Autoscaler autoscaler;
  private final Database db;
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
      logger.info("Connecting to ffwd at {}:{}", ffwdHost, ffwdPort);
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
    final AutoscalerMetrics autoscalerMetrics = new AutoscalerMetrics(registry);
    db = new PostgresDatabase(config.getConfig("database"), autoscalerMetrics);
    final URI uri = new URI("http://0.0.0.0:" + port);
    final ResourceConfig resourceConfig =
        new AutoscaleResourceConfig(
            SERVICE_NAME,
            config,
            new ClusterResources(
                db,
                cluster -> BigtableUtil.createSession(cluster.instanceId(), cluster.projectId())),
            new HealthCheck(db));
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
        logger.error("Failed to create new instance of cluster filter " + clusterFilterClass, e);
      }
    }

    autoscaler =
        new Autoscaler(
            Executors.newFixedThreadPool(CONCURRENCY_LIMIT),
            stackdriverClient,
            db,
            autoscalerMetrics,
            clusterFilter);

    executor.scheduleWithFixedDelay(
        autoscaler, RUN_INTERVAL.toMillis(), RUN_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);

    autoscalerMetrics.scheduleCleanup(db);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    onShutdown();
                  } catch (final IOException | InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                  }
                }));

    registry.register(
        APP_PREFIX.tagged("what", "enabled-clusters"),
        (Gauge<Long>) () -> db.getBigtableClusters().stream().filter(p -> p.enabled()).count());

    registry.register(
        APP_PREFIX.tagged("what", "disabled-clusters"),
        (Gauge<Long>) () -> db.getBigtableClusters().stream().filter(p -> !p.enabled()).count());

    registry.register(
        APP_PREFIX.tagged("what", "open-file-descriptors"),
        (Gauge<Long>)
            () ->
                ((UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                    .getOpenFileDescriptorCount());

    registry.register(
        APP_PREFIX.tagged("what", "daily-resize-count"),
        (Gauge<Long>) () -> db.getDailyResizeCount());

    for (final ErrorCode code : ErrorCode.values()) {
      registry.register(
          APP_PREFIX.tagged("what", "failing-cluster-count").tagged("error-code", code.name()),
          (Gauge<Long>)
              () ->
                  db.getBigtableClusters()
                      .stream()
                      .filter(p -> p.enabled())
                      .filter(p -> p.errorCode().orElse(ErrorCode.OK) == code)
                      .filter(p -> p.consecutiveFailureCount() > 0)
                      .count());
    }

    server.start();
  }

  private void onShutdown() throws IOException, ExecutionException, InterruptedException {
    try {
      stackdriverClient.close();
    } catch (final Exception e) {
      logger.error("Exception while closing stackdriverClient", e);
    }
    server.shutdown(10, TimeUnit.SECONDS).get();
    if (reporter != null) {
      reporter.stop();
    }

    executor.shutdown();
    try {
      executor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      logger.error("Exception while awaiting executor termination", e);
    }
    logger.info("ScheduledExecutor stopped");
    autoscaler.close();
    logger.info("Bigtable sessions and Stackdriver sessions have been closed");
    try {
      db.close();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    logger.info("Database connection closed");
  }
}
