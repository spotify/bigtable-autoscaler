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

import com.typesafe.config.Config;
import org.glassfish.jersey.server.ResourceConfig;

public class AutoscaleResourceConfig extends ResourceConfig {

  public AutoscaleResourceConfig(
      final String serviceName, final Config config, final Object... resources) {
    setApplicationName(serviceName);
    for (final Object o : resources) {
      register(o);
    }
    if (config.hasPath("additionalPackages")) {
      packages(config.getStringList("additionalPackages").toArray(new String[0]));
    }
    if (config.hasPath("additionalClasses")) {
      property("jersey.config.server.provider.classnames", config.getString("additionalClasses"));
    }
  }
}
