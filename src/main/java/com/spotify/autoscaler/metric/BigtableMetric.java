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

package com.spotify.autoscaler.metric;

import com.codahale.metrics.Gauge;
import com.spotify.autoscaler.ClusterData;
import com.spotify.autoscaler.db.Database;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class BigtableMetric {

  public static List<String> getAllMetrics() {
    List<String> metrics = new ArrayList<>();
    for (Metrics metric : Metrics.values()) {
      metrics.add(metric.tag);
    }
    for (LoadMetricType loadMetricType : LoadMetricType.values()) {
      metrics.add(loadMetricType.tag);
    }
    for (ErrorCode errorCode : ErrorCode.values()) {
      metrics.add(errorCode.tag);
    }
    return metrics;
  }

  public enum Metrics {
    NODE_COUNT("node-count") {
      public Gauge getMetricValue(final ClusterData clusterData, final Database db) {
        return clusterData::getCurrentNodeCount;
      }
    },
    MIN_NODE_COUNT("min-node-count") {
      public Gauge getMetricValue(final ClusterData clusterData, final Database db) {
        return clusterData::getMinNodeCount;
      }
    },
    MAX_NODE_COUNT("max-node-count") {
      public Gauge getMetricValue(final ClusterData clusterData, final Database db) {
        return clusterData::getMaxNodeCount;
      }
    },
    EFFECTIVE_MIN_NODE_COUNT("effective-min-node-count") {
      public Gauge getMetricValue(final ClusterData clusterData, final Database db) {
        return clusterData::getEffectiveMinNodeCount;
      }
    },
    LAST_CHECK_TIME("last-check-time") {
      public Gauge getMetricValue(final ClusterData clusterData, final Database db) {
        return () ->
            db.getBigtableCluster(
                    clusterData.getCluster().projectId(),
                    clusterData.getCluster().instanceId(),
                    clusterData.getCluster().clusterId())
                .flatMap(
                    p ->
                        Optional.of(
                            Duration.between(p.lastCheck().orElse(Instant.EPOCH), Instant.now())))
                .get()
                .getSeconds();
      }
    },
    CPU_TARGET_RATIO("cpu-target-ratio") {
      public Gauge getMetricValue(final ClusterData clusterData, final Database db) {
        return () -> clusterData.getCpuUtil() / clusterData.getCluster().cpuTarget();
      }
    };

    public String tag;

    Metrics(final String tag) {
      this.tag = tag;
    }

    public abstract Gauge getMetricValue(final ClusterData clusterData, final Database db);
  }

  public enum LoadMetricType {
    CPU("cpu-util"),
    STORAGE("storage-util");

    public String tag;

    LoadMetricType(final String tag) {
      this.tag = tag;
    }
  }

  public enum ErrorCode {
    CONSECUTIVE_FAILURE_COUNT("consecutive-failure-count");

    public String tag;

    ErrorCode(final String tag) {
      this.tag = tag;
    }
  }
}
