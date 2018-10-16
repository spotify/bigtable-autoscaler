package com.spotify.autoscaler.filters;

import com.spotify.autoscaler.db.BigtableCluster;

public class AllowAllClusterFilter implements ClusterFilter {
  @Override
  public boolean match(BigtableCluster cluster) {
    return true;
  }
}
