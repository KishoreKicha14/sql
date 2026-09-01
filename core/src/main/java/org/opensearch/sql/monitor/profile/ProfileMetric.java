/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

/** Metric for query profiling. */
public interface ProfileMetric {
  /**
   * @return metric name.
   */
  String name();

  /**
   * @return current metric value.
   */
  long value();

  /**
   * Increment the metric by the given delta.
   *
   * @param delta amount to add
   */
  void add(long delta);

  /**
   * Set the metric to the provided value.
   *
   * @param value new metric value
   */
  void set(long value);

  /**
   * Record one phase execution's resource consumption. The elapsed time is added to {@link
   * #value()} (identical to {@link #add(long)}), and the CPU time and allocated memory are
   * accumulated separately so a phase can report time, CPU, and memory together.
   *
   * @param timeNanos elapsed wall-clock nanoseconds
   * @param cpuNanos CPU nanoseconds consumed (0 if unavailable)
   * @param memoryBytes bytes allocated (0 if unavailable)
   */
  default void record(long timeNanos, long cpuNanos, long memoryBytes) {
    add(timeNanos);
  }

  /**
   * @return accumulated CPU nanoseconds across recorded executions, or 0 if never recorded.
   */
  default long cpuNanos() {
    return 0L;
  }

  /**
   * @return accumulated allocated bytes across recorded executions, or 0 if never recorded.
   */
  default long memoryBytes() {
    return 0L;
  }
}
