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
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ClusterMetricsDataGenerator {

  private static final String PROJECT_ID = "test-project-id";
  public static final String INSTANCE_ID = "test-instance-id";
  public static final String CLUSTER_ID = "test-cluster-id";
  public static final String PATH_METRICS = "src/test/resources";

  private static final String NODE_COUNT =
      "metric.type=\"bigtable.googleapis.com/cluster/node_count\""
          + " AND resource.labels.project_id=\"%s\" AND resource.labels.instance=\"%s\" AND resource.labels.cluster=\"%s\"";

  private static final String DISK_USAGE_METRIC =
      "metric.type=\"bigtable.googleapis.com/cluster/storage_utilization\""
          + " AND resource.labels.project_id=\"%s\" AND resource.labels.instance=\"%s\" AND resource.labels.cluster=\"%s\"";

  private static final String RECEIVED_BYTES_METRIC =
      "metric.type=\"bigtable.googleapis.com/server/received_bytes_count\""
          + " AND resource.labels.project_id=\"%s\" AND resource.labels.instance=\"%s\" AND resource.labels.cluster=\"%s\"";

  private static final String SENT_BYTES_METRIC =
      "metric.type=\"bigtable.googleapis.com/server/sent_bytes_count\""
          + " AND resource.labels.project_id=\"%s\" AND resource.labels.instance=\"%s\" AND resource.labels.cluster=\"%s\"";

  public static void main(String[] args) throws IOException {
    final BigtableCluster cluster =
        new BigtableClusterBuilder()
            .projectId(PROJECT_ID)
            .instanceId(INSTANCE_ID)
            .clusterId(CLUSTER_ID)
            .build();

    final TimeInterval interval = interval(Duration.ofHours(24));

    Map<Instant, ClusterMetricsData> metrics = new HashMap<>(1440); // Resolution: minutes
    populateIntervalWithValue(interval, metrics, p -> new ClusterMetricsData());

    final MetricServiceClient metricServiceClient = MetricServiceClient.create();
    populateDiskUtilization(metricServiceClient, metrics, cluster, interval);
    populateNodeCount(metricServiceClient, metrics, cluster, interval);
    populateReceivedBytes(metricServiceClient, metrics, cluster, interval);
    populateSentBytes(metricServiceClient, metrics, cluster, interval);

    // save metrics as json
    try (FileWriter file =
        new FileWriter(
            Paths.get(
                    PATH_METRICS,
                    String.format("%s_%s_%s.json", PROJECT_ID, INSTANCE_ID, CLUSTER_ID))
                .toString())) {
      file.write(new ObjectMapper().writeValueAsString(metrics));
      file.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void populateSentBytes(
      final MetricServiceClient metricServiceClient,
      final Map<Instant, ClusterMetricsData> metrics,
      final BigtableCluster cluster,
      final TimeInterval interval) {

    PagedResponseWrappers.ListTimeSeriesPagedResponse response =
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
            new ClusterMetricsData(
                existing.diskUtilization,
                existing.nodeCount,
                existing.writeOpsCount,
                existing.readOpsCount,
                existing.receivedBytes,
                Double.sum(existing.sentBytes, newValue)));
  }

  private static void populateReceivedBytes(
      final MetricServiceClient metricServiceClient,
      final Map<Instant, ClusterMetricsData> metrics,
      final BigtableCluster cluster,
      final TimeInterval interval) {

    PagedResponseWrappers.ListTimeSeriesPagedResponse response =
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
            new ClusterMetricsData(
                existing.diskUtilization,
                existing.nodeCount,
                existing.writeOpsCount,
                existing.readOpsCount,
                Double.sum(existing.receivedBytes, newValue),
                existing.sentBytes));
  }

  private static void populateNodeCount(
      final MetricServiceClient metricServiceClient,
      final Map<Instant, ClusterMetricsData> metrics,
      final BigtableCluster cluster,
      final TimeInterval interval) {

    PagedResponseWrappers.ListTimeSeriesPagedResponse response =
        metricServiceClient.listTimeSeries(
            ProjectName.of(cluster.projectId()),
            String.format(
                NODE_COUNT, cluster.projectId(), cluster.instanceId(), cluster.clusterId()),
            interval,
            ListTimeSeriesRequest.TimeSeriesView.FULL);

    aggregate(
        response,
        metrics,
        v -> (double) v.getInt64Value(),
        (existing, newValue) ->
            new ClusterMetricsData(
                existing.diskUtilization,
                Math.max(existing.nodeCount, newValue),
                existing.writeOpsCount,
                existing.readOpsCount,
                existing.receivedBytes,
                existing.sentBytes));
  }

  private static void populateDiskUtilization(
      final MetricServiceClient metricServiceClient,
      final Map<Instant, ClusterMetricsData> metrics,
      final BigtableCluster bigtableCluster,
      final TimeInterval interval) {

    PagedResponseWrappers.ListTimeSeriesPagedResponse response =
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
            new ClusterMetricsData(
                Math.max(existing.diskUtilization, newValue),
                existing.nodeCount,
                existing.writeOpsCount,
                existing.readOpsCount,
                existing.receivedBytes,
                existing.sentBytes));
  }

  private static void aggregate(
      final PagedResponseWrappers.ListTimeSeriesPagedResponse response,
      final Map<Instant, ClusterMetricsData> metrics,
      final Function<TypedValue, Double> converter,
      final BiFunction<ClusterMetricsData, Double, ClusterMetricsData> valueCalculator) {

    for (PagedResponseWrappers.ListTimeSeriesPage page : response.iteratePages()) {
      for (TimeSeries ts : page.getValues()) {
        for (Point p : ts.getPointsList()) {
          Double value = converter.apply(p.getValue());
          populateIntervalWithValue(p.getInterval(), metrics, q -> valueCalculator.apply(q, value));
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

    for (Instant i = startMinute; !i.isAfter(endMinute); i = i.plus(1, ChronoUnit.MINUTES)) {
      result.put(i, valueCalculator.apply(result.get(i)));
    }
  }

  private static TimeInterval interval(final Duration duration) {
    long currentTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

    final Timestamp pastTimestamp =
        Timestamp.newBuilder().setSeconds(currentTimeSeconds - duration.getSeconds()).build();
    final Timestamp currentTimestamp =
        Timestamp.newBuilder().setSeconds(currentTimeSeconds).build();
    return TimeInterval.newBuilder()
        .setStartTime(pastTimestamp)
        .setEndTime(currentTimestamp)
        .build();
  }

  public static class ClusterMetricsData {
    public Double diskUtilization = 0.0d;
    public Double nodeCount = 0.0d;
    public Double writeOpsCount = 0.0d;
    public Double readOpsCount = 0.0d;
    public Double receivedBytes = 0.0d;
    public Double sentBytes = 0.0d;

    private ClusterMetricsData(
        final Double diskUtilization,
        final Double nodeCount,
        final Double writeOpsCount,
        final Double readOpsCount,
        final Double receivedBytes,
        final Double sentBytes) {
      this.diskUtilization = diskUtilization;
      this.nodeCount = nodeCount;
      this.writeOpsCount = writeOpsCount;
      this.readOpsCount = readOpsCount;
      this.receivedBytes = receivedBytes;
      this.sentBytes = sentBytes;
    }

    private ClusterMetricsData() {}
  }
}
