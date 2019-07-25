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

import com.spotify.autoscaler.Autoscaler;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.filters.ClusterFilter;
import com.spotify.autoscaler.metric.AutoscalerMetrics;
import dagger.Module;
import dagger.Provides;
import java.util.concurrent.Executors;

@Module
public class AutoscalerModule {
  private static final int CONCURRENCY_LIMIT = 5;

  @Provides
  public Autoscaler autoscaler(
      final StackdriverClient stackdriverClient,
      final Database database,
      final AutoscalerMetrics autoscalerMetrics,
      final ClusterFilter clusterFilter) {
    return new Autoscaler(
        Executors.newFixedThreadPool(CONCURRENCY_LIMIT),
        stackdriverClient,
        database,
        autoscalerMetrics,
        clusterFilter);
  }
}
