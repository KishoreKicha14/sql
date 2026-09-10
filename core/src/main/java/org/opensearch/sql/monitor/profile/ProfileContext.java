/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

/** Context for collecting profiling metrics during query execution. */
public interface ProfileContext {
  /**
   * @return whether this context collects per-phase metrics (time/CPU/memory). This is cheap and,
   *     for the always-on Query Insights capture, true on every query.
   */
  boolean isEnabled();

  /**
   * Whether the expensive per-operator plan-node profiling is enabled. When true, the query plan is
   * rewritten with per-node profiling wrappers ({@code ProfileEnumerableRel}) that instrument every
   * row iteration to record per-node time and row counts — the costly part of {@code profile=true}.
   * This is independent of {@link #isEnabled()} per-phase capture: a query can collect cheap
   * per-phase resource metrics without paying for plan-node instrumentation.
   *
   * @return true only when full plan-node profiling was requested (i.e. {@code profile=true})
   */
  default boolean isPlanProfilingEnabled() {
    return false;
  }

  /**
   * Obtain or create a metric with the provided name.
   *
   * @param name fully qualified metric name
   * @return metric instance
   */
  ProfileMetric getOrCreateMetric(MetricName name);

  /**
   * Register the root plan node for profiling.
   *
   * @param planRoot root plan node
   */
  void setPlanRoot(ProfilePlanNode planRoot);

  /** TODO: merge with planRoot into one generic execution-engine-specific plan profile field. */
  default void setEnginePlan(Object plan) {}

  /**
   * Finalize profiling and return a snapshot.
   *
   * @return immutable query profile snapshot
   */
  QueryProfile finish();
}
