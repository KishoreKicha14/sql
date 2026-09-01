/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

import java.util.concurrent.atomic.LongAdder;

/** Concrete metric backed by {@link LongAdder}. */
final class DefaultMetricImpl implements ProfileMetric {

  private final String name;
  private final LongAdder value = new LongAdder();
  private final LongAdder cpuNanos = new LongAdder();
  private final LongAdder memoryBytes = new LongAdder();

  /**
   * Construct a metric with the provided name.
   *
   * @param name metric name
   */
  DefaultMetricImpl(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public long value() {
    return value.sum();
  }

  @Override
  public void add(long delta) {
    value.add(delta);
  }

  @Override
  public void set(long value) {
    this.value.reset();
    this.value.add(value);
  }

  @Override
  public void record(long timeNanos, long cpuNanos, long memoryBytes) {
    this.value.add(timeNanos);
    // Negative deltas can occur if the JVM reports -1 for an unsupported metric; ignore those.
    if (cpuNanos > 0) {
      this.cpuNanos.add(cpuNanos);
    }
    if (memoryBytes > 0) {
      this.memoryBytes.add(memoryBytes);
    }
  }

  @Override
  public long cpuNanos() {
    return cpuNanos.sum();
  }

  @Override
  public long memoryBytes() {
    return memoryBytes.sum();
  }
}
