package com.spotify.autoscaler;

import com.spotify.autoscaler.db.BigtableCluster;
import com.spotify.autoscaler.db.ClusterResizeLogBuilder;
import java.time.Duration;

public interface Algorithm {

  ScalingEvent calculateWantedNodes(final BigtableCluster cluster,
                           final ClusterResizeLogBuilder clusterResizeLogBuilder,
                           final Duration samplingDuration,
                           final int currentNodes);
}
