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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.function.Supplier;

public class FakeBTCluster {

  private final Supplier<Instant> timeSource;
  private int nodes;
  private Map<Instant, ClusterMetricsDataGenerator.ClusterMetricsData> metrics;

  public FakeBTCluster(final Supplier<Instant> timeSource, Map<Instant, ClusterMetricsDataGenerator.ClusterMetricsData> metrics) {

    this.timeSource = timeSource;
    this.metrics = metrics;
  }

  void setNumberOfNodes(final int nodes) {
    this.nodes = nodes;
  }

  public double getCPU() {
    final ClusterMetricsDataGenerator.ClusterMetricsData currentMetrics = getMetricsForNow();
    return 0.5;
  }

  public double getStorage() {
    return getMetricsForNow().diskUtilization;
  }

  private ClusterMetricsDataGenerator.ClusterMetricsData getMetricsForNow() {
    final Instant now = timeSource.get();
    final Instant nowMinute = now.truncatedTo(ChronoUnit.MINUTES);
    return metrics.get(nowMinute);
  }
}
