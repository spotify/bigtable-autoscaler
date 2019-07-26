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

import com.spotify.autoscaler.db.BigtableCluster;
import org.slf4j.MDC;

public class LoggerContext {

  public static void pushContext(final BigtableCluster cluster) {
    MDC.put("cluster.projectId", cluster.projectId());
    MDC.put("cluster.instanceId", cluster.instanceId());
    MDC.put("cluster.clusterId", cluster.clusterId());
  }

  public static void clearContext() {
    MDC.clear();
  }
}
