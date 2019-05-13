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

import io.norberg.automatter.AutoMatter;
import java.util.Date;
import java.util.Optional;

@AutoMatter
public interface ClusterResizeLog {

  Date timestamp();

  String projectId();

  String instanceId();

  String clusterId();

  int minNodes();

  int maxNodes();

  int loadDelta();

  double cpuTarget();

  Optional<Integer> overloadStep();

  int currentNodes();

  int targetNodes();

  double cpuUtilization();

  double storageUtilization();

  boolean success();

  String resizeReason();

  Optional<String> errorMessage();
}
