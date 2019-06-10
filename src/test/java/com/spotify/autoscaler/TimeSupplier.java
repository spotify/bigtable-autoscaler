package com.spotify.autoscaler;

import java.time.Instant;
import java.util.function.Supplier;

public class TimeSupplier implements Supplier<Instant> {

  private Instant time;

  public void setTime(final Instant time) {
    this.time = time;
  }

  @Override
  public Instant get() {
    return time;
  }
}