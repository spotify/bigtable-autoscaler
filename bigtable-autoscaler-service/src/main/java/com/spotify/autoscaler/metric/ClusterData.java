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

import com.spotify.autoscaler.ErrorCode;
import com.spotify.autoscaler.db.BigtableCluster;
import io.norberg.automatter.AutoMatter;
import java.util.Optional;

@AutoMatter
public interface ClusterData {

  BigtableCluster cluster();

  int currentNodeCount();

  int minNodeCount();

  int maxNodeCount();

  int effectiveMinNodeCount();

  double cpuUtil();

  int consecutiveFailureCount();

  double storageUtil();

  Optional<ErrorCode> lastErrorCode();
}
