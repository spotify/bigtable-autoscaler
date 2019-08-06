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
import com.spotify.autoscaler.db.ErrorCode;
import java.util.Map;

public enum ErrorGauges {
  CONSECUTIVE_FAILURE_COUNT("consecutive-failure-count") {
    @Override
    public Gauge getMetricValue(
        final Map<String, ClusterData> registeredClusters,
        final String clusterName,
        final ErrorCode code) {
      return () -> {
        final ClusterData clusterData = registeredClusters.get(clusterName);
        return clusterData.lastErrorCode().orElse(ErrorCode.OK) == code
            ? clusterData.consecutiveFailureCount()
            : 0;
      };
    }
  };

  private final String tag;

  ErrorGauges(final String tag) {
    this.tag = tag;
  }

  public String getTag() {
    return tag;
  }

  public abstract Gauge getMetricValue(
      final Map<String, ClusterData> registeredClusters,
      final String clusterName,
      final ErrorCode code);
}
