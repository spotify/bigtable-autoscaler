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
import com.spotify.autoscaler.db.Database;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

public enum ClusterDataGauges {
  NODE_COUNT("node-count") {
    public Gauge getMetricValue(
        final Map<String, ClusterData> registeredClusters,
        final String clusterName,
        final Database db) {
      return () -> {
        ClusterData clusterData = registeredClusters.get(clusterName);
        return clusterData.currentNodeCount();
      };
    }
  },
  MIN_NODE_COUNT("min-node-count") {
    public Gauge getMetricValue(
        final Map<String, ClusterData> registeredClusters,
        final String clusterName,
        final Database db) {
      return () -> {
        ClusterData clusterData = registeredClusters.get(clusterName);
        return clusterData.minNodeCount();
      };
    }
  },
  MAX_NODE_COUNT("max-node-count") {
    public Gauge getMetricValue(
        final Map<String, ClusterData> registeredClusters,
        final String clusterName,
        final Database db) {
      return () -> {
        ClusterData clusterData = registeredClusters.get(clusterName);
        return clusterData.maxNodeCount();
      };
    }
  },
  EFFECTIVE_MIN_NODE_COUNT("effective-min-node-count") {
    public Gauge getMetricValue(
        final Map<String, ClusterData> registeredClusters,
        final String clusterName,
        final Database db) {
      return () -> {
        ClusterData cluster = registeredClusters.get(clusterName);
        return cluster.effectiveMinNodeCount();
      };
    }
  },
  LAST_CHECK_TIME("last-check-time") {
    public Gauge getMetricValue(
        final Map<String, ClusterData> registeredClusters,
        final String clusterName,
        final Database db) {
      return () -> {
        ClusterData clusterData = registeredClusters.get(clusterName);
        return db.getBigtableCluster(
                clusterData.cluster().projectId(),
                clusterData.cluster().instanceId(),
                clusterData.cluster().clusterId())
            .flatMap(
                p ->
                    Optional.of(
                        Duration.between(p.lastCheck().orElse(Instant.EPOCH), Instant.now())))
            .get()
            .getSeconds();
      };
    }
  },
  CPU_TARGET_RATIO("cpu-target-ratio") {
    public Gauge getMetricValue(
        final Map<String, ClusterData> registeredClusters,
        final String clusterName,
        final Database db) {
      return () -> {
        ClusterData clusterData = registeredClusters.get(clusterName);
        return clusterData.cpuUtil() / clusterData.cluster().cpuTarget();
      };
    }
  },

  STORAGE_TARGET_RATIO("storage-target-ratio") {
    public Gauge getMetricValue(
        final Map<String, ClusterData> registeredClusters,
        final String clusterName,
        final Database db) {
      return () -> {
        ClusterData clusterData = registeredClusters.get(clusterName);
        return clusterData.storageUtil() / clusterData.cluster().storageTarget();
      };
    }
  };

  private String tag;

  public String getTag() {
    return tag;
  }

  ClusterDataGauges(final String tag) {
    this.tag = tag;
  }

  public abstract Gauge getMetricValue(
      final Map<String, ClusterData> registeredClusters,
      final String clusterName,
      final Database db);
}
