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

package com.spotify.autoscaler.api.type;

import com.google.gson.JsonElement;
import com.google.gson.annotations.SerializedName;
import java.util.Map;

public class ReconcileRequest {

  private final String version;
  private final Map<String, Map<String, JsonElement>> owns;
  private final Boolean deletionInProgress;

  @SerializedName("for")
  private BigtableAutoscaler forObject;

  public ReconcileRequest(final String version,
                          final Map<String, Map<String, JsonElement>> owns, final Boolean deletionInProgress,
                          final BigtableAutoscaler forObject) {
    this.version = version;
    this.owns = owns;
    this.deletionInProgress = deletionInProgress;
    this.forObject = forObject;
  }

  public String getVersion() {
    return version;
  }

  public Map<String, Map<String, JsonElement>> getOwns() {
    return owns;
  }

  public Boolean getDeletionInProgress() {
    return deletionInProgress;
  }

  public BigtableAutoscaler getForObject() {
    return forObject;
  }
}
