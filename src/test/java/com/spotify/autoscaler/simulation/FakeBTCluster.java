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

package com.spotify.autoscaler.simulation;

import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testcontainers.shaded.com.fasterxml.jackson.core.type.TypeReference;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

public class FakeBTCluster {

  public static final String METRICS_PATH = "src/test/resources/simulated_clusters";
  public static final String FILE_PATTERN = "%s_%s_%s.json";
  public static final Pattern FILE_PATTERN_RE =
      Pattern.compile(FILE_PATTERN.replace("%s", "([A-Za-z0-9-]+)"));
  private final Supplier<Instant> timeSource;
  private int nodes;
  private final Map<Instant, ClusterMetricsData> metrics;
  private BigtableCluster cluster;

  public FakeBTCluster(final Supplier<Instant> timeSource, final BigtableCluster cluster) {

    this.timeSource = timeSource;
    this.cluster = cluster;
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
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    final Map<Instant, ClusterMetricsData> metrics = new HashMap<>();
    tmp.forEach((k, v) -> metrics.put(Instant.parse(k), v));
    return metrics;
  }

  public Instant getFirstValidMetricsInstant() {
    // Sometimes Stackdriver returns empty metrics. Autoscaler should normally be able to handle
    // them gracefully.
    // However, to be able to correctly initialize the FakeBTCluster with an initial node count, we
    // need the
    // first metrics that will serve as the beginning instant for the test to have a valid node
    // count.
    final Map.Entry<Instant, ClusterMetricsData> firstMetrics =
        metrics
            .entrySet()
            .stream()
            .sorted(Comparator.comparing(Map.Entry::getKey))
            .filter(e -> e.getValue().nodeCount() > 0)
            .findFirst()
            .get();
    return firstMetrics.getKey();
  }

  public static Path getFilePathForCluster(final BigtableCluster cluster) {
    return Paths.get(
        METRICS_PATH,
        String.format(
            FILE_PATTERN, cluster.projectId(), cluster.instanceId(), cluster.clusterId()));
  }

  public static BigtableClusterBuilder getClusterBuilderForFilePath(final Path path) {
    final Matcher matcher = FILE_PATTERN_RE.matcher(path.getFileName().toString());
    if (matcher.find()) {
      final String project = matcher.group(1);
      final String instance = matcher.group(2);
      final String cluster = matcher.group(3);
      return new BigtableClusterBuilder()
          .projectId(project)
          .instanceId(instance)
          .clusterId(cluster);
    }
    throw new RuntimeException("Invalid file: " + path.toString());
  }

  public BigtableCluster getCluster() {
    this.cluster =
        BigtableClusterBuilder.from(cluster).loadDelta(getMetricsForNow().loadDelta.intValue()).build();
    return this.cluster;
  }

  public Supplier<Instant> getTimeSource() {
    return this.timeSource;
  }

  public int getNumberOfNodes() {
    return this.nodes;
  }

  public void setNumberOfNodes(final int nodes) {
    this.cluster = BigtableClusterBuilder.from(cluster).lastChange(timeSource.get()).build();
    this.nodes = nodes;
  }

  public double getCPU() {
    final ClusterMetricsData currentMetrics = getMetricsForNow();
    // TODO(gizem): calculate the simulated cpu from metrics + nodes
    return currentMetrics.cpuLoad() * currentMetrics.nodeCount() / nodes;
  }

  public double getStorage() {
    final ClusterMetricsData metricsForNow = getMetricsForNow();
    return metricsForNow.diskUtilization() * metricsForNow.nodeCount() / nodes;
  }

  public ClusterMetricsData getMetricsForNow() {
    final Instant now = timeSource.get();
    final Instant nowMinute = now.truncatedTo(ChronoUnit.MINUTES);
    return metrics.get(nowMinute);
  }

  @Override
  public String toString() {
    return String.format(
        "%s/%s/%s", cluster.projectId(), cluster.instanceId(), cluster.clusterId());
  }
}
