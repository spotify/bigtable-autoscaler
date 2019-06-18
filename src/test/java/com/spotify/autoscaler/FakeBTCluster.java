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

import com.spotify.autoscaler.db.BigtableCluster;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.testcontainers.shaded.com.fasterxml.jackson.core.type.TypeReference;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

public class FakeBTCluster {

  public static final String METRICS_PATH = "src/test/resources/simulated_clusters";
  private final Supplier<Instant> timeSource;
  private int nodes;
  private Map<Instant, ClusterMetricsData> metrics;

  public FakeBTCluster(final Supplier<Instant> timeSource, final BigtableCluster cluster) {

    this.timeSource = timeSource;
    this.metrics = getMetrics(cluster);
  }

  private Map<Instant, ClusterMetricsData> getMetrics(final BigtableCluster cluster) {

    final ObjectMapper jsonMapper = new ObjectMapper();
    final Map<String, ClusterMetricsData> tmp;
    try {
      tmp =
          jsonMapper.readValue(
              getFilePathForCluster(cluster).toFile(),
              new TypeReference<Map<String, ClusterMetricsData>>() {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final Map<Instant, ClusterMetricsData> metrics = new HashMap<>();
    tmp.forEach((k, v) -> metrics.put(Instant.parse(k), v));
    return metrics;
  }

  public static Path getFilePathForCluster(final BigtableCluster cluster) {
    return Paths.get(
        METRICS_PATH,
        String.format(
            "%s_%s_%s.json", cluster.projectId(), cluster.instanceId(), cluster.clusterId()));
  }

  void setNumberOfNodes(final int nodes) {
    this.nodes = nodes;
  }

  public double getCPU() {
    final ClusterMetricsData currentMetrics = getMetricsForNow();
    // TODO(gizem): calculate the simulated cpu from metrics + nodes
    return currentMetrics.cpuLoad() * currentMetrics.nodeCount() / nodes;
  }

  public double getStorage() {
    final ClusterMetricsData metricsForNow = getMetricsForNow();
    return Math.ceil(metricsForNow.diskUtilization() * metricsForNow.nodeCount() / nodes);
  }

  private ClusterMetricsData getMetricsForNow() {
    final Instant now = timeSource.get();
    final Instant nowMinute = now.truncatedTo(ChronoUnit.MINUTES);
    return metrics.get(nowMinute);
  }
}
