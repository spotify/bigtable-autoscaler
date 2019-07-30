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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.params.provider.Arguments;

public class FakeBigtableCluster implements Arguments {

  private final Supplier<Instant> timeSource;
  private final Map<Instant, ClusterMetricsData> metrics;

  private BigtableCluster cluster;
  private int nodes;

  public FakeBigtableCluster(
      final File path, final Supplier<Instant> timeSource, final BigtableCluster defaults) {
    this.timeSource = timeSource;
    this.cluster = defaults;
    this.metrics = getMetrics(path);
  }

  private Map<Instant, ClusterMetricsData> getMetrics(final File file) {

    final ObjectMapper jsonMapper = new ObjectMapper();
    final Map<String, ClusterMetricsData> tmp;
    try {
      tmp = jsonMapper.readValue(file, new TypeReference<Map<String, ClusterMetricsData>>() {});
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
    // However, to be able to correctly initialize the FakeBigtableCluster with an initial node
    // count, we
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

  // DONTLIKEIT
  // make it clear that we return a modified version of the cluster, like simulating a read from the
  // database
  public BigtableCluster getCluster() {
    this.cluster =
        BigtableClusterBuilder.from(cluster)
            .loadDelta(getMetricsForNow().loadDelta.intValue())
            .build();
    return this.cluster;
  }

  public Supplier<Instant> getTimeSource() {
    return this.timeSource;
  }

  public int getNumberOfNodes() {
    return this.nodes;
  }

  // DONTLIKEIT
  // make it clear that we are simulating a write to the database
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

  @Override
  public Object[] get() {
    return new Object[] {this};
  }
}
