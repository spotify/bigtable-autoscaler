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

package com.spotify.autoscaler.simulation;

import com.google.common.collect.Lists;
import com.spotify.autoscaler.TimeSupplier;
import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import java.io.File;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FakeBigtableClusterLoader {

  private static final String FILE_PATTERN = "%s_%s_%s.json";
  private static final Pattern FILE_PATTERN_RE =
      Pattern.compile(FILE_PATTERN.replace("%s", "([A-Za-z0-9-]+)"));

  public static final String SIMPLE = "simulated_clusters/project_instance_no-jobs.json";
  public static final String ONE_JOB = "simulated_clusters/project3_instance3_one-job.json";
  public static final String ONE_JOB_2 = "simulated_clusters/project4_instance4_one-job.json";
  public static final String MANY_JOBS = "simulated_clusters/project2_instance2_many-jobs.json";
  public static final String MANY_JOBS_2 = "simulated_clusters/project5_instance5_many-jobs.json";
  public static final String REPLICATION =
      "simulated_clusters/project6_instance6_no-job-only-replication.json";
  public static final String REPLICATION_WITH_ONE_JOB =
      "simulated_clusters/project6_instance6_one-job-replicated.json";

  public static FakeBigtableCluster one(
      final String fileName, final BigtableClusterBuilder defaults) {
    final ClassLoader loader = Thread.currentThread().getContextClassLoader();
    final File file = new File(loader.getResource(fileName).getFile());
    final BigtableCluster cluster =
        defaults
            .clusterId(fieldFromFileName(file, 3))
            .instanceId(fieldFromFileName(file, 2))
            .projectId(fieldFromFileName(file, 1))
            .build();

    return new FakeBigtableCluster(file, new TimeSupplier(), cluster);
  }

  public static List<FakeBigtableCluster> all(final BigtableClusterBuilder defaults) {
    final List<String> files =
        Lists.newArrayList(
            SIMPLE,
            ONE_JOB,
            ONE_JOB_2,
            MANY_JOBS,
            MANY_JOBS_2,
            REPLICATION,
            REPLICATION_WITH_ONE_JOB);

    return files.stream().map(file -> one(file, defaults)).collect(Collectors.toList());
  }

  private static String fieldFromFileName(final File file, final int field) {
    final Matcher matcher = FILE_PATTERN_RE.matcher(file.getName());
    if (matcher.find()) {
      return matcher.group(field);
    }
    throw new RuntimeException("Invalid file: " + file.getName());
  }
}
