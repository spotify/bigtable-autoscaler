package com.spotify.autoscaler.filters;

import com.spotify.autoscaler.db.BigtableCluster;

public interface ClusterFilter {
  boolean match(BigtableCluster cluster);
}
