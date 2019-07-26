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
import com.spotify.autoscaler.db.BigtableClusterBuilder;
import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SimulatedClusterLoader {

  public static final String METRICS_PATH = "simulated_clusters";
  public static final String FILE_PATTERN = "%s_%s_%s.json";
  public static final Pattern FILE_PATTERN_RE =
      Pattern.compile(FILE_PATTERN.replace("%s", "([A-Za-z0-9-]+)"));

  private static File[] getResourceFolderFiles(final String folder) {
    final ClassLoader loader = Thread.currentThread().getContextClassLoader();
    final URL url = loader.getResource(folder);
    final String path = url.getPath();
    return new File(path).listFiles();
  }

  public static List<FakeBTCluster> loadAll(final BigtableClusterBuilder defaults) {
    final File[] files = getResourceFolderFiles(METRICS_PATH);
    return Arrays.stream(files)
        .map(
            file -> {
              final BigtableCluster cluster =
                  defaults
                      .clusterId(fieldFromFileName(file, 3))
                      .instanceId(fieldFromFileName(file, 2))
                      .projectId(fieldFromFileName(file, 1))
                      .build();
              return new FakeBTCluster(file.toPath(), new TimeSupplier(), cluster);
            })
        .collect(Collectors.toList());
  }

  private static String fieldFromFileName(final File file, final int field) {
    final Matcher matcher = FILE_PATTERN_RE.matcher(file.getName());
    if (matcher.find()) {
      return matcher.group(field);
    }
    throw new RuntimeException("Invalid file: " + file.getName());
  }
}
