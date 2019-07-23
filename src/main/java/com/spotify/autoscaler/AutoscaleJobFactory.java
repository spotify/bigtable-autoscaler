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

import com.google.cloud.bigtable.grpc.BigtableSession;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.Database;
import com.spotify.autoscaler.metric.AutoscalerMetrics;
import java.io.IOException;
import java.time.Instant;
import java.util.function.Supplier;

public class AutoscaleJobFactory {

  public AutoscaleJob createAutoscaleJob(
      final BigtableSession bigtableSession,
      final IOSupplier<StackdriverClient> stackdriverClient,
      final BigtableCluster cluster,
      final Database db,
      final AutoscalerMetrics autoscalerMetrics,
      final Supplier<Instant> timeSource)
      throws IOException {
    return new AutoscaleJob(
        bigtableSession, stackdriverClient.get(), cluster, db, autoscalerMetrics, timeSource);
  }

  public interface IOSupplier<T> {

    T get() throws IOException;
  }
}
