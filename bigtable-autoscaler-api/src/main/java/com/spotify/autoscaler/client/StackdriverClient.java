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

package com.spotify.autoscaler.client;

import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.PagedResponseWrappers;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.protobuf.Timestamp;
import com.spotify.autoscaler.db.BigtableCluster;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

public interface StackdriverClient extends Closeable {

  static PagedResponseWrappers.ListTimeSeriesPagedResponse getMetric(
      final MetricServiceClient metricServiceClient,
      final BigtableCluster bigtableCluster,
      final TimeInterval interval,
      final String metricName) {
    return metricServiceClient.listTimeSeries(
        ProjectName.of(bigtableCluster.projectId()),
        String.format(
            metricName,
            bigtableCluster.projectId(),
            bigtableCluster.instanceId(),
            bigtableCluster.clusterId()),
        interval,
        ListTimeSeriesRequest.TimeSeriesView.FULL);
  }

  @NotNull
  static TimeInterval interval(final Duration duration) {
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

  Double getDiskUtilization(BigtableCluster bigtableCluster, Duration duration);

  Double getCpuLoad(BigtableCluster bigtableCluster, Duration duration);

  @Override
  void close() throws IOException;
}
