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
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.PagedResponseWrappers;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.monitoring.v3.TypedValue;
import com.google.protobuf.Timestamp;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ClusterMetricsDataGenerator {

  private static final String PROJECT_ID = "project";
  private static final String INSTANCE_ID = "instance";
  private static final String CLUSTER_ID = "cluster";

  private static final String CLUSTER_FILTER =
      "resource.labels.project_id=\"%s\" AND resource.labels.instance=\"%s\""
          + " AND resource.labels.cluster=\"%s\"";

  private static final String NODE_COUNT_METRIC =
      String.format(
          "metric.type=\"bigtable.googleapis.com/cluster/node_count\" " + "AND %s", CLUSTER_FILTER);

  private static final String DISK_USAGE_METRIC =
      String.format(
          "metric.type=\"bigtable.googleapis.com/cluster/storage_utilization\" " + "AND %s",
          CLUSTER_FILTER);

  private static final String CPU_LOAD_METRIC =
      String.format(
          "metric.type=\"bigtable.googleapis.com/cluster/cpu_load\" " + "AND %s", CLUSTER_FILTER);

  private static final String RECEIVED_BYTES_METRIC =
      String.format(
          "metric.type=\"bigtable.googleapis.com/server/received_bytes_count\" " + "AND %s",
          CLUSTER_FILTER);

  private static final String SENT_BYTES_METRIC =
      String.format(
          "metric.type=\"bigtable.googleapis.com/server/sent_bytes_count\" " + "AND %s",
          CLUSTER_FILTER);

  private static final String REQUEST_COUNT_METRIC =
      String.format(
          "metric.type=\"bigtable.googleapis.com/server/request_count\" " + "AND %s",
          CLUSTER_FILTER);

  private static final String MODIFIED_ROWS_METRIC =
      String.format(
          "metric.type=\"bigtable.googleapis.com/server/modified_rows_count\" " + "AND %s",
          CLUSTER_FILTER);

  private static final String RETURNED_ROWS_METRIC =
      String.format(
          "metric.type=\"bigtable.googleapis.com/server/returned_rows_count\" " + "AND %s",
          CLUSTER_FILTER);

  private static final String ERROR_COUNT_METRIC =
      String.format(
          "metric.type=\"bigtable.googleapis.com/server/error_count\" " + "AND %s", CLUSTER_FILTER);

  public static void main(final String[] args) throws IOException {
    final BigtableCluster cluster =
        new BigtableClusterBuilder()
            .projectId(PROJECT_ID)
            .instanceId(INSTANCE_ID)
            .clusterId(CLUSTER_ID)
            .build();

    final TimeInterval interval = interval(Duration.ofHours(24));

    final Map<Instant, ClusterMetricsData> metrics = new HashMap<>(1440); // Resolution: minutes
    populateIntervalWithValue(interval, metrics, p -> ClusterMetricsData.builder().build());

    final MetricServiceClient metricServiceClient = MetricServiceClient.create();
    populateDiskUtilization(metricServiceClient, metrics, cluster, interval);
    populateNodeCount(metricServiceClient, metrics, cluster, interval);
    populateReceivedBytes(metricServiceClient, metrics, cluster, interval);
    populateSentBytes(metricServiceClient, metrics, cluster, interval);
    populateCPULoad(metricServiceClient, metrics, cluster, interval);
    populateRequestCount(metricServiceClient, metrics, cluster, interval);
    populateModifiedRows(metricServiceClient, metrics, cluster, interval);
    populateReturnedRows(metricServiceClient, metrics, cluster, interval);
    populateErrorCount(metricServiceClient, metrics, cluster, interval);

    final Map<String, ClusterMetricsData> data = new HashMap<>();
    metrics.forEach((k, v) -> data.put(k.toString(), v));

    // save metrics as json
    try (final FileWriter file =
        new FileWriter(FakeBTCluster.getFilePathForCluster(cluster).toString())) {
      file.write(new ObjectMapper().writeValueAsString(data));
      file.flush();
    } catch (final IOException e) {
      e.printStackTrace();
    }
  }

  private static void populateErrorCount(
      final MetricServiceClient metricServiceClient,
      final Map<Instant, ClusterMetricsData> metrics,
      final BigtableCluster cluster,
      final TimeInterval interval) {

    final PagedResponseWrappers.ListTimeSeriesPagedResponse response =
        metricServiceClient.listTimeSeries(
            ProjectName.of(cluster.projectId()),
            String.format(
                ERROR_COUNT_METRIC, cluster.projectId(), cluster.instanceId(), cluster.clusterId()),
            interval,
            ListTimeSeriesRequest.TimeSeriesView.FULL);

    aggregate(
        response,
        metrics,
        v -> (double) v.getInt64Value(),
        (existing, newValue) ->
            ClusterMetricsData.ClusterMetricsDataBuilder.from(existing)
                .errorCount(Double.sum(existing.errorCount(), newValue))
                .build(),
        true);
  }

  private static void populateReturnedRows(
      final MetricServiceClient metricServiceClient,
      final Map<Instant, ClusterMetricsData> metrics,
      final BigtableCluster cluster,
      final TimeInterval interval) {

    final PagedResponseWrappers.ListTimeSeriesPagedResponse response =
        metricServiceClient.listTimeSeries(
            ProjectName.of(cluster.projectId()),
            String.format(
                RETURNED_ROWS_METRIC,
                cluster.projectId(),
                cluster.instanceId(),
                cluster.clusterId()),
            interval,
            ListTimeSeriesRequest.TimeSeriesView.FULL);

    aggregate(
        response,
        metrics,
        v -> (double) v.getInt64Value(),
        (existing, newValue) ->
            ClusterMetricsData.ClusterMetricsDataBuilder.from(existing)
                .returnedRows(Double.sum(existing.returnedRows(), newValue))
                .build(),
        true);
  }

  private static void populateModifiedRows(
      final MetricServiceClient metricServiceClient,
      final Map<Instant, ClusterMetricsData> metrics,
      final BigtableCluster cluster,
      final TimeInterval interval) {

    final PagedResponseWrappers.ListTimeSeriesPagedResponse response =
        metricServiceClient.listTimeSeries(
            ProjectName.of(cluster.projectId()),
            String.format(
                MODIFIED_ROWS_METRIC,
                cluster.projectId(),
                cluster.instanceId(),
                cluster.clusterId()),
            interval,
            ListTimeSeriesRequest.TimeSeriesView.FULL);

    aggregate(
        response,
        metrics,
        v -> (double) v.getInt64Value(),
        (existing, newValue) ->
            ClusterMetricsData.ClusterMetricsDataBuilder.from(existing)
                .modifiedRows(Double.sum(existing.modifiedRows(), newValue))
                .build(),
        true);
  }

  private static void populateRequestCount(
      final MetricServiceClient metricServiceClient,
      final Map<Instant, ClusterMetricsData> metrics,
      final BigtableCluster cluster,
      final TimeInterval interval) {

    final PagedResponseWrappers.ListTimeSeriesPagedResponse response =
        metricServiceClient.listTimeSeries(
            ProjectName.of(cluster.projectId()),
            String.format(
                REQUEST_COUNT_METRIC,
                cluster.projectId(),
                cluster.instanceId(),
                cluster.clusterId()),
            interval,
            ListTimeSeriesRequest.TimeSeriesView.FULL);

    aggregate(
        response,
        metrics,
        v -> (double) v.getInt64Value(),
        (existing, newValue) ->
            ClusterMetricsData.ClusterMetricsDataBuilder.from(existing)
                .requestCount(Double.sum(existing.requestCount(), newValue))
                .build(),
        true);
  }

  private static void populateCPULoad(
      final MetricServiceClient metricServiceClient,
      final Map<Instant, ClusterMetricsData> metrics,
      final BigtableCluster cluster,
      final TimeInterval interval) {

    final PagedResponseWrappers.ListTimeSeriesPagedResponse response =
        metricServiceClient.listTimeSeries(
            ProjectName.of(cluster.projectId()),
            String.format(
                CPU_LOAD_METRIC, cluster.projectId(), cluster.instanceId(), cluster.clusterId()),
            interval,
            ListTimeSeriesRequest.TimeSeriesView.FULL);

    aggregate(
        response,
        metrics,
        TypedValue::getDoubleValue,
        (existing, newValue) ->
            ClusterMetricsData.ClusterMetricsDataBuilder.from(existing)
                .cpuLoad(Math.max(existing.cpuLoad(), newValue))
                .build());
  }

  private static void populateSentBytes(
      final MetricServiceClient metricServiceClient,
      final Map<Instant, ClusterMetricsData> metrics,
      final BigtableCluster cluster,
      final TimeInterval interval) {

    final PagedResponseWrappers.ListTimeSeriesPagedResponse response =
        metricServiceClient.listTimeSeries(
            ProjectName.of(cluster.projectId()),
            String.format(
                SENT_BYTES_METRIC, cluster.projectId(), cluster.instanceId(), cluster.clusterId()),
            interval,
            ListTimeSeriesRequest.TimeSeriesView.FULL);

    aggregate(
        response,
        metrics,
        v -> (double) v.getInt64Value(),
        (existing, newValue) ->
            ClusterMetricsData.ClusterMetricsDataBuilder.from(existing)
                .sentBytes(Double.sum(existing.sentBytes(), newValue))
                .build(),
        true);
  }

  private static void populateReceivedBytes(
      final MetricServiceClient metricServiceClient,
      final Map<Instant, ClusterMetricsData> metrics,
      final BigtableCluster cluster,
      final TimeInterval interval) {

    final PagedResponseWrappers.ListTimeSeriesPagedResponse response =
        metricServiceClient.listTimeSeries(
            ProjectName.of(cluster.projectId()),
            String.format(
                RECEIVED_BYTES_METRIC,
                cluster.projectId(),
                cluster.instanceId(),
                cluster.clusterId()),
            interval,
            ListTimeSeriesRequest.TimeSeriesView.FULL);

    aggregate(
        response,
        metrics,
        v -> (double) v.getInt64Value(),
        (existing, newValue) ->
            ClusterMetricsData.ClusterMetricsDataBuilder.from(existing)
                .receivedBytes(Double.sum(existing.receivedBytes(), newValue))
                .build(),
        true);
  }

  private static void populateNodeCount(
      final MetricServiceClient metricServiceClient,
      final Map<Instant, ClusterMetricsData> metrics,
      final BigtableCluster cluster,
      final TimeInterval interval) {

    final PagedResponseWrappers.ListTimeSeriesPagedResponse response =
        metricServiceClient.listTimeSeries(
            ProjectName.of(cluster.projectId()),
            String.format(
                NODE_COUNT_METRIC, cluster.projectId(), cluster.instanceId(), cluster.clusterId()),
            interval,
            ListTimeSeriesRequest.TimeSeriesView.FULL);

    aggregate(
        response,
        metrics,
        v -> (double) v.getInt64Value(),
        (existing, newValue) ->
            ClusterMetricsData.ClusterMetricsDataBuilder.from(existing)
                .nodeCount(Math.max(existing.nodeCount(), newValue))
                .build());
  }

  private static void populateDiskUtilization(
      final MetricServiceClient metricServiceClient,
      final Map<Instant, ClusterMetricsData> metrics,
      final BigtableCluster bigtableCluster,
      final TimeInterval interval) {

    final PagedResponseWrappers.ListTimeSeriesPagedResponse response =
        metricServiceClient.listTimeSeries(
            ProjectName.of(bigtableCluster.projectId()),
            String.format(
                DISK_USAGE_METRIC,
                bigtableCluster.projectId(),
                bigtableCluster.instanceId(),
                bigtableCluster.clusterId()),
            interval,
            ListTimeSeriesRequest.TimeSeriesView.FULL);

    aggregate(
        response,
        metrics,
        TypedValue::getDoubleValue,
        (existing, newValue) ->
            ClusterMetricsData.ClusterMetricsDataBuilder.from(existing)
                .diskUtilization(Math.max(existing.diskUtilization(), newValue))
                .build());
  }

  private static void aggregate(
      final PagedResponseWrappers.ListTimeSeriesPagedResponse response,
      final Map<Instant, ClusterMetricsData> metrics,
      final Function<TypedValue, Double> converter,
      final BiFunction<ClusterMetricsData, Double, ClusterMetricsData> valueCalculator) {
    aggregate(response, metrics, converter, valueCalculator, false);
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

  private static TimeInterval interval(final Duration duration) {
    final long currentTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

    final Timestamp pastTimestamp =
        Timestamp.newBuilder().setSeconds(currentTimeSeconds - duration.getSeconds()).build();
    final Timestamp currentTimestamp =
        Timestamp.newBuilder().setSeconds(currentTimeSeconds).build();
    return TimeInterval.newBuilder()
        .setStartTime(pastTimestamp)
        .setEndTime(currentTimestamp)
        .build();
  }
}
