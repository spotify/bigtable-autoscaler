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

import static com.spotify.autoscaler.metric.AutoscalerMetrics.APP_PREFIX;

import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.metric.AutoscalerMetrics;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.metrics.ffwd.FastForwardReporter;
import com.typesafe.config.Config;
import dagger.Module;
import dagger.Provides;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Module
public class MetricsModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsModule.class);

  @Provides
  @Singleton
  public static SemanticMetricRegistry registry() {
    return new SemanticMetricRegistry();
  }

  @Provides
  public FastForwardReporter fastForwardReporter(
      final Config config, final SemanticMetricRegistry registry) {
    final String ffwdHost = config.getString("ffwd.host");
    final int ffwdPort = config.getInt("ffwd.port");

    if (!ffwdHost.isEmpty()) {
      LOGGER.info("Connecting to ffwd at {}:{}", ffwdHost, ffwdPort);
      try {
        return FastForwardReporter.forRegistry(registry)
            .prefix(APP_PREFIX)
            .host(ffwdHost)
            .port(ffwdPort)
            .schedule(TimeUnit.SECONDS, 5)
            .build();
      } catch (IOException e) {
        LOGGER.error("Failed to initialize ffwd reporter", e);
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  private static AutoscalerMetrics autoscalerMetrics;

  @Provides
  public AutoscalerMetrics initializeMetrics(
      final SemanticMetricRegistry registry, final Database database) {
    if (autoscalerMetrics == null) {
      autoscalerMetrics = new AutoscalerMetrics(registry);
      autoscalerMetrics.registerActiveClusters(database);
      autoscalerMetrics.registerOpenFileDescriptors();
      autoscalerMetrics.registerDailyResizeCount(database);
      autoscalerMetrics.registerFailureCount(database);
      autoscalerMetrics.registerOpenDatabaseConnections(database);
      autoscalerMetrics.scheduleCleanup(database);
    }
    return autoscalerMetrics;
  }
}
