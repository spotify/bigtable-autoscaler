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

import com.spotify.metrics.ffwd.FastForwardReporter;
import io.grpc.Server;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.glassfish.grizzly.http.server.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {
  public static final String SERVICE_NAME = "bigtable-autoscaler";
  private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
  private final ScheduledExecutorService scheduledExecutorService =
      new ScheduledThreadPoolExecutor(1);
  private final Autoscaler autoscaler;
  private final HttpServer httpServer;
  private final Server grpcServer;
  private final Optional<FastForwardReporter> reporter;
  private static final Duration RUN_INTERVAL = Duration.ofSeconds(5);

  @Inject
  public Application(
      final Autoscaler autoscaler,
      final HttpServer httpServer,
      final Server grpcServer,
      final Optional<FastForwardReporter> reporter) {
    this.autoscaler = autoscaler;
    this.httpServer = httpServer;
    this.grpcServer = grpcServer;
    this.reporter = reporter;
  }

  public void start() throws IOException {
    reporter.ifPresent(FastForwardReporter::start);
    scheduledExecutorService.scheduleWithFixedDelay(
        autoscaler, RUN_INTERVAL.toMillis(), RUN_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    httpServer.start();
    grpcServer.start();
    addShutdownHooks();
  }

  private void addShutdownHooks() {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    onShutdown();
                    LOGGER.info("services shutdown");
                  } catch (final Exception e) {
                    LOGGER.error("Exception occurred on shutdown", e);
                    throw new RuntimeException(e);
                  }
                }));
  }

  private void onShutdown() throws Exception {
    httpServer.shutdown(10, TimeUnit.SECONDS).get();
    grpcServer.shutdown();
    reporter.ifPresent(FastForwardReporter::stop);
    scheduledExecutorService.shutdown();
    scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS);
    autoscaler.close();
  }
}
