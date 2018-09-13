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

package com.spotify.autoscaler.db;

import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public interface Database extends AutoCloseable {

  Collection<BigtableCluster> getBigtableClusters();

  Optional<BigtableCluster> getBigtableCluster(String projectId, String instanceId, String clusterId);

  boolean insertBigtableCluster(BigtableCluster cluster);

  boolean updateBigtableCluster(BigtableCluster cluster);

  boolean deleteBigtableCluster(String projectId, String instanceId, String clusterId);

  boolean setLastChange(String projectId, String instanceId, String clusterId, Instant lastChange);

  Optional<BigtableCluster> getCandidateCluster();

  boolean clearFailureCount(String projectId, String instanceId, String clusterId);

  boolean increaseFailureCount(String projectId, String instanceId, String clusterId, Instant lastFailure,
                               String lastFailureMessage);

  default Set<String> getActiveClusterKeys() {
    return getBigtableClusters()
        .stream()
        .filter(BigtableCluster::enabled)
        .map(BigtableCluster::clusterName)
        .collect(Collectors.toSet());
  }

  void logResize(ClusterResizeLog log);

  long getDailyResizeCount();

  void healthCheck();

  Collection<ClusterResizeLog> getLatestResizeEvents(String projectId, String instanceId, String clusterId);

  boolean updateLoadDelta(String projectId, String instanceId, String clusterId, Integer loadDelta);
}
