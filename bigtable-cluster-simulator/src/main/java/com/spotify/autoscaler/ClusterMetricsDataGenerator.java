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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.PagedResponseWrappers;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.monitoring.v3.TypedValue;
import com.spotify.autoscaler.client.StackdriverClient;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ClusterMetricsDataGenerator {

  private static final String PROJECT_ID = "project";
  private static final String INSTANCE_ID = "instance";
  private static final String CLUSTER_ID = "cluster";

  private static class DataJobInformation {

    private DataJobInformation(
        final String dataJobStart, final String dataJobEnd, final Double dataJobMinNodesOverride) {
      this.dataJobStart = dataJobStart;
      this.dataJobEnd = dataJobEnd;
      this.dataJobMinNodesOverride = dataJobMinNodesOverride;
    }

    String dataJobStart;
    String dataJobEnd;
    Double dataJobMinNodesOverride;
  }

  // for now we don't have reliable data, so we manually set data job start, duration, etc
  private static final List<DataJobInformation> jobs = Collections.EMPTY_LIST;

  public static void main(final String[] args) throws IOException {
    final BigtableCluster cluster =
        new BigtableClusterBuilder()
            .projectId(PROJECT_ID)
            .instanceId(INSTANCE_ID)
            .clusterId(CLUSTER_ID)
            .build();

    final TimeInterval interval = StackdriverClient.interval(Duration.ofHours(24));

    // create all the data point and initialize them
    final Map<Instant, ClusterMetricsData> metrics = new TreeMap<>();
    populateIntervalWithValue(interval, metrics, unused -> ClusterMetricsData.builder().build());

    // update the data points with real metric data
    final MetricServiceClient metricServiceClient = MetricServiceClient.create();
    for (final Metric metric : Metric.values()) {
      populateMetric(metricServiceClient, metrics, cluster, interval, metric);
    }

    // infer the value of minNodesOverride from the existing metrics, i.e. try to guess if a data
    // job
    // started at some point
    populateMinNodesOverride(metrics);

    // save metrics as json
    final ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    try (final FileWriter file = new FileWriter("TODO")) {
      file.write(mapper.writeValueAsString(metrics));
      file.flush();
    } catch (final IOException e) {
      e.printStackTrace();
    }
  }

  private static void populateMinNodesOverride(final Map<Instant, ClusterMetricsData> metrics) {
    for (final DataJobInformation job : jobs) {
      final Instant dataJobStart = Instant.parse(job.dataJobStart);
      dataJobStart.truncatedTo(ChronoUnit.MINUTES);
      final Instant dataJobEnd = Instant.parse(job.dataJobEnd);
      dataJobEnd.truncatedTo(ChronoUnit.MINUTES);

      for (Instant instant = dataJobStart;
          instant.isBefore(dataJobEnd);
          instant = instant.plus(1, ChronoUnit.MINUTES)) {
        metrics.computeIfPresent(
            instant,
            (unused, current) ->
                ClusterMetricsData.ClusterMetricsDataBuilder.from(current)
                    .minNodesOverride(
                        Math.max(current.minNodesOverride(), job.dataJobMinNodesOverride))
                    .build());
      }
    }
  }

  private static void populateMetric(
      final MetricServiceClient metricServiceClient,
      final Map<Instant, ClusterMetricsData> metrics,
      final BigtableCluster cluster,
      final TimeInterval interval,
      final Metric metric) {

    aggregate(
        StackdriverClient.getMetric(metricServiceClient, cluster, interval, metric.queryString()),
        metrics,
        metric.typeConverter(),
        metric.valueAggregator(),
        metric.shouldBeDistributed());
  }

  private static void aggregate(
      final PagedResponseWrappers.ListTimeSeriesPagedResponse response,
      final Map<Instant, ClusterMetricsData> metrics,
      final Function<TypedValue, Double> converter,
      final BiFunction<ClusterMetricsData, Double, ClusterMetricsData> valueCalculator,
      final boolean distribute) {

    for (final PagedResponseWrappers.ListTimeSeriesPage page : response.iteratePages()) {
      for (final TimeSeries ts : page.getValues()) {
        for (final Point p : ts.getPointsList()) {
          final Double value = converter.apply(p.getValue());
          if (distribute) {
            final Instant startInstant =
                Instant.ofEpochSecond(p.getInterval().getStartTime().getSeconds());
            final Instant endInstant =
                Instant.ofEpochSecond(p.getInterval().getEndTime().getSeconds());
            final long minutesBetween =
                Duration.between(startInstant, endInstant).abs().toMinutes();
            final double distributedValue = value / minutesBetween;
            populateIntervalWithValue(
                p.getInterval(), metrics, q -> valueCalculator.apply(q, distributedValue));
          } else {
            populateIntervalWithValue(
                p.getInterval(), metrics, q -> valueCalculator.apply(q, value));
          }
        }
      }
    }
  }

  private static void populateIntervalWithValue(
      final TimeInterval interval,
      final Map<Instant, ClusterMetricsData> result,
      final Function<ClusterMetricsData, ClusterMetricsData> valueCalculator) {

    final Instant startInstant = Instant.ofEpochSecond(interval.getStartTime().getSeconds());
    final Instant startMinute = startInstant.truncatedTo(ChronoUnit.MINUTES);
    final Instant endInstant = Instant.ofEpochSecond(interval.getEndTime().getSeconds());
    final Instant endMinute = endInstant.truncatedTo(ChronoUnit.MINUTES);

    for (Instant i = startMinute;
        i.isBefore(endMinute) || i.equals(startMinute);
        i = i.plus(1, ChronoUnit.MINUTES)) {
      result.put(i, valueCalculator.apply(result.get(i)));
    }
  }
}
