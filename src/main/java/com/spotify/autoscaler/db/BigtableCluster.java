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

import com.spotify.autoscaler.util.ErrorCode;
import io.norberg.automatter.AutoMatter;
import java.time.Instant;
import java.util.Optional;

@AutoMatter
public interface BigtableCluster {

  String projectId();

  String instanceId();

  String clusterId();

  int minNodes();

  int maxNodes();

  double cpuTarget();

  Optional<Instant> lastChange();

  Optional<Instant> lastCheck();

  boolean enabled();

  Optional<Integer> overloadStep();

  Optional<Instant> lastFailure();

  Optional<String>
      lastFailureMessage(); // Old failure messages will still be kept even if the last attempt
  // worked

  int consecutiveFailureCount(); // 0 means last autoscale attempt succeeded

  int loadDelta(); // 0 means no extra load to consider, gt 0 means minNodes is effectively
  // minNodes+loadDelta

  Optional<ErrorCode> errorCode();

  Optional<Integer> minNodesOverride();

  default String clusterName() {
    return "projects/" + projectId() + "/instances/" + instanceId() + "/clusters/" + clusterId();
  }

  default int effectiveMinNodes() {
    return Math.min(minNodesOverride().orElse(minNodes()), maxNodes());
  }

  default boolean exists() {
    final ErrorCode errorCode = errorCode().orElse(ErrorCode.OK);
    return errorCode != ErrorCode.GRPC_NOT_FOUND && errorCode != ErrorCode.PROJECT_NOT_FOUND;
  }
}
