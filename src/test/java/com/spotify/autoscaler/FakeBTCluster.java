package com.spotify.autoscaler;

import java.time.Instant;
import java.util.function.Supplier;

public class FakeBTCluster {

  private final Supplier<Instant> timeSource;
  private int nodes;

  public FakeBTCluster(final Supplier<Instant> timeSource) {

    this.timeSource = timeSource;
  }

  void setNumberOfNodes(final int nodes) {
    this.nodes = nodes;
  }

  public double getCPU() {
    return 0.5;
  }

  public double getStorage() {
    return 0.5;
  }
}
