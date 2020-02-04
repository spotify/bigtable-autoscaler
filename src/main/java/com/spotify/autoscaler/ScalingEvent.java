package com.spotify.autoscaler;

public class ScalingEvent {
  private int desiredNodeCount;
  private String reason;

  public ScalingEvent(final int desiredNodeCount, final String reason) {
    this.desiredNodeCount = desiredNodeCount;
    this.reason = reason;
  }

  public int getDesiredNodeCount() {
    return desiredNodeCount;
  }

  public String getReason() {
    return reason;
  }
}
