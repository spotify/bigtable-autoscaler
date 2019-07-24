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

package com.spotify.autoscaler.util;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.spotify.autoscaler.Main;
import com.spotify.autoscaler.db.BigtableCluster;
import java.io.IOException;
import java.time.Duration;
import org.slf4j.MDC;

public class BigtableUtil {

  private static final int SHORT_TIMEOUT = (int) Duration.ofSeconds(10).toMillis();
  private static final int LONG_TIMEOUT = (int) Duration.ofSeconds(60).toMillis();
  private static final boolean USE_TIMEOUT = true;

  public static BigtableSession createSession(final String instanceId, final String projectId)
      throws IOException {
    final BigtableInstanceName bigtableInstanceName =
        new BigtableInstanceName(projectId, instanceId);

    final BigtableOptions options =
        new BigtableOptions.Builder()
            .setDataChannelCount(64)
            .setProjectId(projectId)
            .setInstanceId(bigtableInstanceName.getInstanceId())
            .setUserAgent(Main.SERVICE_NAME)
            .setCallOptionsConfig(new CallOptionsConfig(USE_TIMEOUT, SHORT_TIMEOUT, LONG_TIMEOUT))
            .setBulkOptions(
                new BulkOptions.Builder()
                    .setMaxInflightRpcs(1000000)
                    .setMaxMemory(Long.MAX_VALUE)
                    .build())
            .build();

    return new BigtableSession(options);
  }

  public static void pushContext(BigtableCluster cluster) {
    MDC.put("cluster.projectId", cluster.projectId());
    MDC.put("cluster.instanceId", cluster.instanceId());
    MDC.put("cluster.clusterId", cluster.clusterId());
  }

  public static void clearContext() {
    MDC.clear();
  }
}
