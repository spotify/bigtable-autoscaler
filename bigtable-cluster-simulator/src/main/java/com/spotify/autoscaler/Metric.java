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

import com.google.monitoring.v3.TypedValue;
import java.util.function.BiFunction;
import java.util.function.Function;

public enum Metric {
  NODE_COUNT_METRIC(
      "bigtable.googleapis.com/cluster/node_count", v -> (double) v.getInt64Value(), false) {
    @Override
    void setMetricValue(
        final ClusterMetricsData.ClusterMetricsDataBuilder builder, final Double newValue) {
      builder.nodeCount(Math.max(builder.nodeCount(), newValue));
    }
  },
  DISK_USAGE_METRIC(
      "bigtable.googleapis.com/cluster/storage_utilization", TypedValue::getDoubleValue, false) {
    @Override
    void setMetricValue(
        final ClusterMetricsData.ClusterMetricsDataBuilder builder, final Double newValue) {
      builder.diskUtilization(Math.max(builder.diskUtilization(), newValue));
    }
  },
  CPU_LOAD_METRIC("bigtable.googleapis.com/cluster/cpu_load", TypedValue::getDoubleValue, false) {
    @Override
    void setMetricValue(
        final ClusterMetricsData.ClusterMetricsDataBuilder builder, final Double newValue) {
      builder.cpuLoad(Math.max(builder.cpuLoad(), newValue));
    }
  },
  RECEIVED_BYTES_METRIC(
      "bigtable.googleapis.com/server/received_bytes_count",
      v -> (double) v.getInt64Value(),
      true) {
    @Override
    void setMetricValue(
        final ClusterMetricsData.ClusterMetricsDataBuilder builder, final Double newValue) {
      builder.receivedBytes(Double.sum(builder.receivedBytes(), newValue));
    }
  },
  SENT_BYTES_METRIC(
      "bigtable.googleapis.com/server/sent_bytes_count", v -> (double) v.getInt64Value(), true) {
    @Override
    void setMetricValue(
        final ClusterMetricsData.ClusterMetricsDataBuilder builder, final Double newValue) {
      builder.sentBytes(Double.sum(builder.sentBytes(), newValue));
    }
  },
  REQUEST_COUNT_METRIC(
      "bigtable.googleapis.com/server/request_count", v -> (double) v.getInt64Value(), true) {
    @Override
    void setMetricValue(
        final ClusterMetricsData.ClusterMetricsDataBuilder builder, final Double newValue) {
      builder.requestCount(Double.sum(builder.requestCount(), newValue));
    }
  },
  MODIFIED_ROWS_METRIC(
      "bigtable.googleapis.com/server/modified_rows_count", v -> (double) v.getInt64Value(), true) {
    @Override
    void setMetricValue(
        final ClusterMetricsData.ClusterMetricsDataBuilder builder, final Double newValue) {
      builder.modifiedRows(Double.sum(builder.modifiedRows(), newValue));
    }
  },
  RETURNED_ROWS_METRIC(
      "bigtable.googleapis.com/server/returned_rows_count", v -> (double) v.getInt64Value(), true) {
    @Override
    void setMetricValue(
        final ClusterMetricsData.ClusterMetricsDataBuilder builder, final Double newValue) {
      builder.returnedRows(Double.sum(builder.returnedRows(), newValue));
    }
  },
  ERROR_COUNT_METRIC(
      "bigtable.googleapis.com/server/error_count", v -> (double) v.getInt64Value(), true) {
    @Override
    void setMetricValue(
        final ClusterMetricsData.ClusterMetricsDataBuilder builder, final Double newValue) {
      builder.errorCount(Double.sum(builder.errorCount(), newValue));
    }
  };

  private static final String CLUSTER_FILTER =
      "resource.labels.project_id=\"%s\" AND resource.labels.instance=\"%s\""
          + " AND resource.labels.cluster=\"%s\"";

  private final String metricType;
  private final Function<TypedValue, Double> typeConverter;
  private final boolean shouldBeDistributed;

  Metric(
      final String metricType,
      final Function<TypedValue, Double> typeConverter,
      final boolean shouldBeDistributed) {
    this.metricType = metricType;
    this.typeConverter = typeConverter;
    this.shouldBeDistributed = shouldBeDistributed;
  }

  public String queryString() {
    return String.format("metric.type=\"%s\" " + "AND %s", metricType, CLUSTER_FILTER);
  }

  abstract void setMetricValue(
      ClusterMetricsData.ClusterMetricsDataBuilder builder, Double newValue);

  public BiFunction<ClusterMetricsData, Double, ClusterMetricsData> valueAggregator() {
    return (existing, newValue) -> {
      final ClusterMetricsData.ClusterMetricsDataBuilder builder =
          ClusterMetricsData.ClusterMetricsDataBuilder.from(existing);
      setMetricValue(builder, newValue);
      return builder.build();
    };
  }

  public boolean shouldBeDistributed() {
    return shouldBeDistributed;
  }

  public Function<TypedValue, Double> typeConverter() {
    return typeConverter;
  }
}
