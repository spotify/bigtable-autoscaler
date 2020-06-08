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
import com.spotify.autoscaler.db.BigtableCluster;
import java.util.List;
import java.util.Map;

// ReconcileResponse is the local definitions of the response that kubebuilder bridge understands.
public class ReconcileResponse {
  @SerializedName("version")
  private String version;

  @SerializedName("ensure")
  private EnsureResources ensure;

  public ReconcileResponse ensure(EnsureResources ensure) {
    this.ensure = ensure;
    return this;
  }

  public static class EnsureResources {
    @SerializedName("for")
    private ForObject forObject;

    @SerializedName("owns")
    private Map<String, Map<String, JsonElement>> owns;

    public EnsureResources forObject(ForObject forObject) {
      this.forObject = forObject;
      return this;
    }

    public EnsureResources owns(Map<String, Map<String, JsonElement>> owns) {
      this.owns = owns;
      return this;
    }

    public static class ForObject {
      @SerializedName("apiVersion")
      private String apiVersion;

      @SerializedName("kind")
      private String kind;

      @SerializedName("namespace")
      private String namespace;

      @SerializedName("name")
      private String name;

      @SerializedName("labels")
      private Map<String, String> labels;

      @SerializedName("annotations")
      private Map<String, String> annotations;

      @SerializedName("status")
      private JsonElement status;

      @SerializedName("deleted")
      private Boolean deleted;

      public ForObject(String apiVersion, String kind, String namespace, String name) {
        this.apiVersion = apiVersion;
        this.kind = kind;
        this.namespace = namespace;
        this.name = name;
      }

      public ForObject labels(Map<String, String> labels) {
        this.labels = labels;
        return this;
      }

      public ForObject annotations(Map<String, String> annotations) {
        this.annotations = annotations;
        return this;
      }

      public ForObject status(JsonElement status) {
        this.status = status;
        return this;
      }

      public ForObject deleted(Boolean deleted) {
        this.deleted = deleted;
        return this;
      }
    }
  }

  public static class Status {
    private int clusterCount;

    public Status(final List<BigtableCluster> clusters) {
      this.clusterCount = clusters.size();
    }
  }

}
